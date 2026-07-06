"""施工包01 · §B5 持久化与重启恢复。

- 多行写入由调用方包 `with conn:` 事务;本模块函数不各自 commit。
- JSON 输出一律 临时文件 + os.replace 原子写。
- load_state():回放 fills(join orders 得 strategy/symbol/side)减去 positions_closed
  已结束的,重建现金与内存持仓——重启后 equity 与崩溃前一致(§B8 测试5)。
- 止损/止盈/累计资金费等 fills 表无法承载的持仓属性,以 JSON 存 sim_meta
  ('open_position_meta' / 'pending_order_meta'),属 §B2 sim_meta「等」的范畴。
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from platforms.single_account.sim.orders import Fill, Order
from platforms.single_account.sim.position import Position

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schema_paper.sql"
PAPER_DB_PATH = Path(__file__).resolve().parents[3] / "data" / "single_account_paper.db"


def open_paper_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = db_path or PAPER_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()
    return conn


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ---------- 单表写入(不 commit,调用方包事务) ----------

def insert_order(conn: sqlite3.Connection, order: Order) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO orders(order_id, strategy, symbol, side, type, qty, "
        "limit_price, created_ts, status, reason) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (order.order_id, order.strategy, order.symbol, order.side, order.type,
         order.qty, order.limit_price, order.created_ts, order.status, order.reason),
    )


def update_order_status(conn: sqlite3.Connection, order_id: str, status: str, reason: str = "") -> None:
    conn.execute("UPDATE orders SET status=?, reason=? WHERE order_id=?", (status, reason, order_id))


def insert_fill(conn: sqlite3.Connection, fill: Fill) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO fills(fill_id, order_id, ts, price, qty, fee, slippage_bps) "
        "VALUES(?,?,?,?,?,?,?)",
        (fill.fill_id, fill.order_id, fill.ts, fill.price, fill.qty, fill.fee, fill.slippage_bps),
    )


def insert_position_closed(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO positions_closed(strategy, symbol, side, qty, entry_ts, entry_price, "
        "exit_ts, exit_price, gross_pnl, fees, funding, net_pnl, r_multiple, exit_reason, "
        "holding_secs, tags_json) VALUES(:strategy,:symbol,:side,:qty,:entry_ts,:entry_price,"
        ":exit_ts,:exit_price,:gross_pnl,:fees,:funding,:net_pnl,:r_multiple,:exit_reason,"
        ":holding_secs,:tags_json)",
        row,
    )


def insert_funding_event(conn: sqlite3.Connection, ts: int, symbol: str, rate: float,
                         pos_qty: float, amount: float) -> None:
    conn.execute("INSERT INTO funding_events(ts, symbol, rate, pos_qty, amount) VALUES(?,?,?,?,?)",
                 (ts, symbol, rate, pos_qty, amount))


def snapshot_equity(conn: sqlite3.Connection, ts: int, equity: float, cash: float,
                    unrealized: float, drawdown: float) -> None:
    conn.execute("INSERT OR REPLACE INTO equity_snapshots(ts, equity, cash, unrealized, drawdown) "
                 "VALUES(?,?,?,?,?)", (ts, equity, cash, unrealized, drawdown))


def insert_decision(conn: sqlite3.Connection, ts: int, strategy: str, symbol: str, action: str,
                    score_json: str, taken: int, skip_reason: str) -> None:
    conn.execute("INSERT INTO decisions(ts, strategy, symbol, action, score_json, taken, skip_reason) "
                 "VALUES(?,?,?,?,?,?,?)", (ts, strategy, symbol, action, score_json, taken, skip_reason))


# ---------- sim_meta ----------

def get_meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM sim_meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT OR REPLACE INTO sim_meta(key, value) VALUES(?,?)", (key, str(value)))


def incr_meta(conn: sqlite3.Connection, key: str, delta: int = 1) -> int:
    current = int(get_meta(conn, key) or 0) + delta
    set_meta(conn, key, str(current))
    return current


def save_runtime_meta(conn: sqlite3.Connection, positions: Dict[Tuple[str, str], Position],
                      pending: List[Order]) -> None:
    pos_meta = {
        f"{k[0]}|{k[1]}": {"stop": p.stop, "tp": p.tp, "entry_fee": p.entry_fee,
                           "funding_paid": p.funding_paid, "tags": p.tags}
        for k, p in positions.items()
    }
    order_meta = {o.order_id: {"stop": o.stop, "tp": o.tp, "reduce_only": o.reduce_only}
                  for o in pending}
    set_meta(conn, "open_position_meta", json.dumps(pos_meta, ensure_ascii=False))
    set_meta(conn, "pending_order_meta", json.dumps(order_meta, ensure_ascii=False))


# ---------- 重启恢复(§B5 load_open_positions) ----------

def load_state(conn: sqlite3.Connection, initial_cash: float
               ) -> Tuple[float, Dict[Tuple[str, str], Position], List[Order]]:
    """返回 (cash, open_positions, pending_orders)。

    cash = initial_cash + 全部 fills 的现金流(买:-价×量-费;卖:+价×量-费)
           + 全部 funding_events.amount(负=支出)。
    持仓 = fills 按 (strategy,symbol) 轧差后净量非零者(positions_closed 已含平仓 fill,
           轧差自然归零),入场价/时间/费取建立净头寸的那笔 fill。
    """
    rows = conn.execute(
        "SELECT o.strategy, o.symbol, o.side, f.ts, f.price, f.qty, f.fee "
        "FROM fills f JOIN orders o ON o.order_id = f.order_id ORDER BY f.ts, f.fill_id"
    ).fetchall()
    cash = float(initial_cash)
    net: Dict[Tuple[str, str], float] = {}
    entry_fill: Dict[Tuple[str, str], Tuple] = {}
    for strategy, symbol, side, ts, price, qty, fee in rows:
        key = (strategy, symbol)
        signed = qty if side == "buy" else -qty
        if side == "buy":
            cash -= price * qty + fee
        else:
            cash += price * qty - fee
        before = net.get(key, 0.0)
        after = before + signed
        net[key] = after
        if before == 0.0 and after != 0.0:
            entry_fill[key] = (ts, price, qty, fee, "long" if after > 0 else "short")

    for row in conn.execute("SELECT amount FROM funding_events"):
        cash += float(row[0])

    pos_meta = json.loads(get_meta(conn, "open_position_meta") or "{}")
    positions: Dict[Tuple[str, str], Position] = {}
    for key, net_qty in net.items():
        if abs(net_qty) < 1e-12:
            continue
        ts, price, qty, fee, side = entry_fill[key]
        meta = pos_meta.get(f"{key[0]}|{key[1]}", {})
        positions[key] = Position(
            strategy=key[0], symbol=key[1], side=side, qty=abs(net_qty),
            entry_ts=int(ts), entry_price=float(price),
            stop=meta.get("stop"), tp=meta.get("tp"),
            entry_fee=float(meta.get("entry_fee", fee)),
            funding_paid=float(meta.get("funding_paid", 0.0)),
            tags=dict(meta.get("tags") or {}),
        )

    order_meta = json.loads(get_meta(conn, "pending_order_meta") or "{}")
    pending: List[Order] = []
    for row in conn.execute(
            "SELECT order_id, strategy, symbol, side, type, qty, limit_price, created_ts, "
            "status, reason FROM orders WHERE status='new' ORDER BY created_ts"):
        extra = order_meta.get(row[0], {})
        pending.append(Order(
            order_id=row[0], strategy=row[1], symbol=row[2], side=row[3], type=row[4],
            qty=row[5], limit_price=row[6], created_ts=row[7], status=row[8], reason=row[9] or "",
            stop=extra.get("stop"), tp=extra.get("tp"),
            reduce_only=bool(extra.get("reduce_only", False)),
        ))
    return cash, positions, pending
