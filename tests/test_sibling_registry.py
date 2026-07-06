"""施工包04 · §3 SiblingOrderRegistry 测试(全 mock,不连交易所)。"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"))

from sibling_registry import SiblingOrderRegistry, resolve_conflict

A = "0xaaaa000000000000000000000000000000000001"
B = "0xbbbb000000000000000000000000000000000002"
TOKEN = "123456789"
PAIRED = "987654321"


def _reg_with_a_sell(price=0.46) -> SiblingOrderRegistry:
    reg = SiblingOrderRegistry()
    reg.register(A, TOKEN, "SELL", price, 100.0, "a-sell-1")
    return reg


# 1. 直接交叉检测(§3.1)
def test_direct_cross_detect():
    reg = _reg_with_a_sell(0.46)
    crossed, hit = reg.would_cross(B, TOKEN, "BUY", 0.46)
    assert crossed and hit["order_id"] == "a-sell-1" and hit["side"] == "SELL"
    assert reg.would_cross(B, TOKEN, "BUY", 0.45) == (False, None)   # 不触及
    assert reg.would_cross(A, TOKEN, "BUY", 0.46) == (False, None)   # 自家不算
    # sell 对称:兄弟 BUY 0.46,我 SELL 0.46 → 交叉;SELL 0.47 → 不交叉
    reg2 = SiblingOrderRegistry()
    reg2.register(A, TOKEN, "BUY", 0.46, 100.0, "a-buy-1")
    assert reg2.would_cross(B, TOKEN, "SELL", 0.46)[0]
    assert not reg2.would_cross(B, TOKEN, "SELL", 0.47)[0]


# 2. adjust 模式(§3.2):目标 buy 0.46 命中 → 实际挂 0.45,adjusted+1
def test_adjust_mode():
    reg = _reg_with_a_sell(0.46)
    crossed, _ = reg.would_cross(B, TOKEN, "BUY", 0.46)
    assert crossed
    action, new_price = resolve_conflict("adjust", "BUY", 0.46, tick=0.01, adjust_ticks=1)
    assert action == "adjust" and abs(new_price - 0.45) < 1e-12
    reg.note_adjusted()
    assert reg.stats()["adjusted"] == 1
    assert not reg.would_cross(B, TOKEN, "BUY", new_price)[0]        # 退让后不再交叉
    # 退让越过约束下限 → 跳过(等同 block)
    action2, price2 = resolve_conflict("adjust", "BUY", 0.46, tick=0.01,
                                       adjust_ticks=1, floor=0.455)
    assert action2 == "skip" and price2 is None
    # sell 对称:向上退让
    action3, price3 = resolve_conflict("adjust", "SELL", 0.46, tick=0.01, adjust_ticks=1)
    assert action3 == "adjust" and abs(price3 - 0.47) < 1e-12


# 3. block 模式(§3.3):跳过挂单,skipped+1,不发单
def test_block_mode():
    reg = _reg_with_a_sell(0.46)
    reg.would_cross(B, TOKEN, "BUY", 0.46)
    action, new_price = resolve_conflict("block", "BUY", 0.46, tick=0.01)
    assert action == "skip" and new_price is None
    reg.note_skipped()
    stats = reg.stats()
    assert stats["skipped"] == 1 and stats["adjusted"] == 0


# 4. observe 模式(§3.4):照常挂原价,仅 conflicts_detected+1(零干预)
def test_observe_mode():
    reg = _reg_with_a_sell(0.46)
    crossed, _ = reg.would_cross(B, TOKEN, "BUY", 0.46)
    assert crossed
    action, new_price = resolve_conflict("observe", "BUY", 0.46, tick=0.01)
    assert action == "proceed" and abs(new_price - 0.46) < 1e-12
    stats = reg.stats()
    assert stats["conflicts_detected"] == 1
    assert stats["adjusted"] == 0 and stats["skipped"] == 0


# 5. 登记生命周期(§3.5)
def test_register_lifecycle():
    reg = SiblingOrderRegistry()
    reg.register(A, TOKEN, "SELL", 0.46, 100.0, "a-1")
    assert reg.would_cross(B, TOKEN, "BUY", 0.46)[0]
    reg.unregister(A, "a-1")                                  # 撤单/成交
    assert not reg.would_cross(B, TOKEN, "BUY", 0.46)[0]
    # sync_token 对账:整体覆盖
    reg.register(A, TOKEN, "SELL", 0.46, 100.0, "a-2")
    reg.sync_token(A, TOKEN, [("a-3", "SELL", 0.48, 50.0)])   # a-2 已不在线
    assert not reg.would_cross(B, TOKEN, "BUY", 0.46)[0]
    assert reg.would_cross(B, TOKEN, "BUY", 0.48)[0]
    # cancel-all → 清空(保护名单除外;exit 单放另一 token 便于区分)
    token2 = "555555555"
    reg.register(A, token2, "SELL", 0.44, 10.0, "a-exit")
    reg.clear_funder(A, keep_order_ids={"a-exit"})
    assert not reg.would_cross(B, TOKEN, "BUY", 0.48)[0]      # a-3 已被清
    assert reg.would_cross(B, token2, "BUY", 0.44)[0]         # exit 单仍受保护登记
    reg.clear_funder(A)
    assert not reg.would_cross(B, token2, "BUY", 0.99)[0]
    assert reg.stats()["live_orders"] == 0


# 6. 线程安全(§3.6):并发 register/would_cross/unregister 各 1000 次
def test_thread_safety():
    reg = SiblingOrderRegistry()
    n = 1000
    errors: list = []

    def worker(funder: str, prefix: str) -> None:
        try:
            for i in range(n):
                oid = f"{prefix}-{i}"
                reg.register(funder, TOKEN, "SELL" if i % 2 else "BUY",
                             0.40 + (i % 20) * 0.01, 10.0, oid)
                reg.would_cross(funder, TOKEN, "BUY", 0.50)
                reg.unregister(funder, oid)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f, p))
               for f, p in ((A, "a"), (B, "b"), ("0xcccc", "c"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    stats = reg.stats()
    assert stats["live_orders"] == 0
    assert stats["registered_total"] == 3 * n
    assert stats["unregistered_total"] == 3 * n
    assert stats["checked"] == 3 * n


# 7. 配对 token 互补 BUY(§3.7):只观察,不拦截
def test_complement_observe():
    reg = SiblingOrderRegistry()
    reg.register(A, PAIRED, "BUY", 0.60, 100.0, "a-paired-buy")
    matched, hit = reg.complement_would_match(B, PAIRED, my_buy_price=0.45)
    assert matched and hit["order_id"] == "a-paired-buy"       # 0.45+0.60 >= 1
    assert not reg.complement_would_match(B, PAIRED, my_buy_price=0.30)[0]  # 0.90 < 1
    assert not reg.complement_would_match(A, PAIRED, my_buy_price=0.45)[0]  # 自家不算
    stats = reg.stats()
    assert stats["complement_observed"] == 1
    assert stats["adjusted"] == 0 and stats["skipped"] == 0    # 不拦截不调整
    # 直接交叉检查也不会把配对 BUY 当冲突(不同 token 不同向)
    assert not reg.would_cross(B, TOKEN, "BUY", 0.99)[0]
