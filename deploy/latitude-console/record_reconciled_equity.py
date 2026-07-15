"""Write a five-minute, cashflow-adjusted Var/Decibel PnL snapshot.

This process is read-only with respect to venues.  It reads the existing four
source snapshots and appends a local reporting record only when reconciliation
is complete.
"""
from __future__ import annotations

import json

from console_app import record_reconciled_pnl_snapshot


if __name__ == "__main__":
    print(json.dumps(record_reconciled_pnl_snapshot(), ensure_ascii=False))
