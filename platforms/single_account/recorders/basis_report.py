"""施工包01 · §A4 极简 basis 统计 CLI。

用法:python -m platforms.single_account.recorders.basis_report --days 1
输出每 symbol:样本数、basis(=mark/ref−1)的 mean/median/p95(bps)、ref 数据源占比。
不做互相关/滞后估计(下一批)。
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time
from pathlib import Path
from statistics import mean, median
from typing import Any, Optional

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.single_account.recorders.common import MARKET_DB_PATH


def p95(values: list) -> float:
    ordered = sorted(values)
    idx = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[idx]


def compute_report(conn: sqlite3.Connection, days: float,
                   now: Optional[float] = None) -> dict:
    cutoff = int((now if now is not None else time.time()) - days * 86400)
    report: dict[str, Any] = {}
    symbols = [row[0] for row in conn.execute(
        "SELECT DISTINCT symbol FROM basis_ticks WHERE ts >= ? ORDER BY symbol", (cutoff,))]
    for symbol in symbols:
        rows = conn.execute(
            "SELECT platform_mark, ref_price, ref_source FROM basis_ticks "
            "WHERE symbol=? AND ts >= ?", (symbol, cutoff)).fetchall()
        basis_bps = [
            (mark / ref - 1.0) * 10000.0
            for mark, ref, _ in rows
            if mark is not None and ref is not None and ref != 0
        ]
        sources: dict[str, int] = {}
        for _, _, source in rows:
            key = str(source or "")
            sources[key] = sources.get(key, 0) + 1
        total = len(rows)
        report[symbol] = {
            "samples": total,
            "basis_bps": {
                "mean": round(mean(basis_bps), 4) if basis_bps else None,
                "median": round(median(basis_bps), 4) if basis_bps else None,
                "p95": round(p95(basis_bps), 4) if basis_bps else None,
                "n": len(basis_bps),
            },
            "ref_source_share": {k: round(v / total, 4) for k, v in sorted(sources.items())} if total else {},
        }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Basis statistics report.")
    parser.add_argument("--days", type=float, default=1.0)
    parser.add_argument("--db", default=str(MARKET_DB_PATH))
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    report = compute_report(conn, args.days)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
