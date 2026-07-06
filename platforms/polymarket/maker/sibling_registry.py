"""施工包04 · SiblingOrderRegistry:跨账号自成交防线(进程内共享)。

背景:multi_runner 单进程跑全部账号,但每个 engine 只识别自家订单——
A 的 exit-SELL 可能被 B 的 BID 吃掉(跨账号自成交:双倍手续费+对敲形态风险)。
本模块提供全账号在线订单登记与"下单前"交叉检查;由 multi_runner 创建单实例
注入每个 engine(engine 单跑时自建空实例,行为不变)。

线程模型:engine 的注册/查询都发生在事件循环协程内(单线程),严格说可省锁;
按规格保留 threading.Lock(成本可忽略,防未来变化)。

模式(config sibling_registry.mode,默认 observe):
  observe — 只记日志与计数,不干预(首次上线跑 24h 看统计);
  adjust  — 向不交叉方向退 adjust_ticks 档;退让越过约束下限则跳过(等同 block);
  block   — 直接跳过本次挂单。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

EPS = 1e-9


@dataclass
class SiblingOrder:
    funder: str
    token_id: str
    side: str          # "BUY" | "SELL"
    price: float
    size: float
    order_id: str
    ts: float


class SiblingOrderRegistry:
    """进程内共享:全部账号的在线订单登记与交叉检查。线程安全(threading.Lock)。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._orders: Dict[str, SiblingOrder] = {}      # order_id -> entry
        self._by_token: Dict[str, Set[str]] = {}        # token_id -> {order_id}
        self._by_funder: Dict[str, Set[str]] = {}       # funder_lc -> {order_id}
        self._stats: Dict[str, int] = {
            "checked": 0, "conflicts_detected": 0, "adjusted": 0, "skipped": 0,
            "complement_checked": 0, "complement_observed": 0,
            "registered_total": 0, "unregistered_total": 0,
        }

    # ---------- 登记生命周期 ----------

    def register(self, funder: str, token_id: str, side: str, price: float,
                 size: float, order_id: str) -> None:
        funder = str(funder or "").lower()
        order_id = str(order_id or "")
        if not funder or not order_id or not token_id:
            return
        entry = SiblingOrder(funder=funder, token_id=str(token_id),
                             side=str(side).upper(), price=float(price),
                             size=float(size), order_id=order_id, ts=time.time())
        with self._lock:
            self._orders[order_id] = entry
            self._by_token.setdefault(entry.token_id, set()).add(order_id)
            self._by_funder.setdefault(funder, set()).add(order_id)
            self._stats["registered_total"] += 1

    def unregister(self, funder: str, order_id: str) -> None:
        self.unregister_many(funder, [order_id])

    def unregister_many(self, funder: str, order_ids: List[str]) -> None:
        funder = str(funder or "").lower()
        with self._lock:
            for oid in order_ids:
                oid = str(oid or "")
                entry = self._orders.get(oid)
                if entry is None or (funder and entry.funder != funder):
                    continue
                self._remove_locked(entry)

    def _remove_locked(self, entry: SiblingOrder) -> None:
        self._orders.pop(entry.order_id, None)
        self._by_token.get(entry.token_id, set()).discard(entry.order_id)
        self._by_funder.get(entry.funder, set()).discard(entry.order_id)
        self._stats["unregistered_total"] += 1

    def sync_token(self, funder: str, token_id: str,
                   entries: List[Tuple[str, str, float, float]]) -> None:
        """refresh 对账:以 entries[(order_id, side, price, size)] 为准,
        整体重建该 (funder, token) 的登记(引擎本地清单是整体刷新模型)。"""
        funder = str(funder or "").lower()
        token_id = str(token_id)
        if not funder or not token_id:
            return
        with self._lock:
            stale = [self._orders[oid] for oid in list(self._by_token.get(token_id, set()))
                     if self._orders.get(oid) and self._orders[oid].funder == funder]
            for entry in stale:
                self._remove_locked(entry)
            now = time.time()
            for order_id, side, price, size in entries:
                oid = str(order_id or "")
                if not oid:
                    continue
                entry = SiblingOrder(funder=funder, token_id=token_id,
                                     side=str(side).upper(), price=float(price),
                                     size=float(size), order_id=oid, ts=now)
                self._orders[oid] = entry
                self._by_token.setdefault(token_id, set()).add(oid)
                self._by_funder.setdefault(funder, set()).add(oid)
                self._stats["registered_total"] += 1

    def clear_funder(self, funder: str, keep_order_ids: Optional[Set[str]] = None) -> None:
        """cancel-all / 关停路径:清空该账号全部登记(可保护 exit 订单)。"""
        funder = str(funder or "").lower()
        keep = {str(x) for x in (keep_order_ids or set()) if x}
        with self._lock:
            for oid in list(self._by_funder.get(funder, set())):
                if oid in keep:
                    continue
                entry = self._orders.get(oid)
                if entry is not None:
                    self._remove_locked(entry)

    # ---------- 交叉检查 ----------

    def would_cross(self, funder: str, token_id: str, side: str,
                    price: float) -> Tuple[bool, Optional[dict]]:
        """该 funder 以 side/price 在 token_id 挂单,是否会与【其他 funder】的对向
        在线订单交叉。buy 交叉:存在兄弟 sell 且 sell.price <= price;sell 对称。
        自己的订单不算。返回 (是否交叉, 命中的兄弟订单信息或 None)。"""
        funder = str(funder or "").lower()
        side = str(side).upper()
        price = float(price)
        with self._lock:
            self._stats["checked"] += 1
            best: Optional[SiblingOrder] = None
            for oid in self._by_token.get(str(token_id), set()):
                entry = self._orders.get(oid)
                if entry is None or entry.funder == funder:
                    continue
                if side == "BUY" and entry.side == "SELL" and entry.price <= price + EPS:
                    if best is None or entry.price < best.price:
                        best = entry
                elif side == "SELL" and entry.side == "BUY" and entry.price >= price - EPS:
                    if best is None or entry.price > best.price:
                        best = entry
            if best is None:
                return False, None
            self._stats["conflicts_detected"] += 1
            return True, {
                "funder": best.funder[:10], "order_id": best.order_id,
                "side": best.side, "price": best.price, "size": best.size,
                "token_id": best.token_id[:16],
            }

    def complement_would_match(self, funder: str, paired_token_id: str,
                               my_buy_price: float) -> Tuple[bool, Optional[dict]]:
        """§2.3 v1 只观察:本账号 BUY 与兄弟在配对 token 上的 BUY 满足
        price_yes + price_no >= 1(可经 mint 互相成交)→ 只计数,不拦截。"""
        funder = str(funder or "").lower()
        with self._lock:
            self._stats["complement_checked"] += 1
            for oid in self._by_token.get(str(paired_token_id), set()):
                entry = self._orders.get(oid)
                if entry is None or entry.funder == funder or entry.side != "BUY":
                    continue
                if float(my_buy_price) + entry.price >= 1.0 - EPS:
                    self._stats["complement_observed"] += 1
                    return True, {
                        "funder": entry.funder[:10], "order_id": entry.order_id,
                        "price": entry.price, "paired_token": entry.token_id[:16],
                    }
        return False, None

    # ---------- 计数 ----------

    def note_adjusted(self) -> None:
        with self._lock:
            self._stats["adjusted"] += 1

    def note_skipped(self) -> None:
        with self._lock:
            self._stats["skipped"] += 1

    def stats(self) -> dict:
        with self._lock:
            out = dict(self._stats)
            out["live_orders"] = len(self._orders)
            return out


def resolve_conflict(mode: str, side: str, price: float, tick: float,
                     adjust_ticks: int = 1, floor: Optional[float] = None,
                     ceiling: Optional[float] = None) -> Tuple[str, Optional[float]]:
    """交叉命中后的处置决策(纯函数,便于单测)。

    返回 (action, new_price):
      observe → ("proceed", 原价):零干预,只由调用方计数;
      adjust  → BUY 退 price−tick×n(低于 floor → ("skip", None));
                SELL 退 price+tick×n(高于 ceiling → ("skip", None));
      block   → ("skip", None)。
    """
    mode = str(mode or "observe").lower()
    side = str(side).upper()
    if mode == "observe":
        return "proceed", float(price)
    if mode == "block":
        return "skip", None
    if mode == "adjust":
        delta = float(tick) * int(adjust_ticks)
        if side == "BUY":
            new_price = float(price) - delta
            if new_price <= 0 or (floor is not None and new_price < float(floor) - EPS):
                return "skip", None
        else:
            new_price = float(price) + delta
            if ceiling is not None and new_price > float(ceiling) + EPS:
                return "skip", None
        return "adjust", new_price
    # 未知 mode → 按 observe 处理(防线配置坏了不应阻断交易)
    return "proceed", float(price)
