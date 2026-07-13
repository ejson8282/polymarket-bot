#!/usr/bin/env python3
"""Export existing Python maker state into the Rust shadow input contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.maker_shadow_export import (  # noqa: E402
    export_polymarket_snapshot,
    export_predictfun_snapshot,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a read-only maker shadow snapshot; never signs or sends orders."
    )
    parser.add_argument("--risk-limits", help="Optional JSON file overriding observation limits.")
    parser.add_argument("--output", required=True, help="Output JSON path.")
    subparsers = parser.add_subparsers(dest="venue", required=True)

    predictfun = subparsers.add_parser("predictfun")
    predictfun.add_argument("--intents", required=True)
    predictfun.add_argument("--actual", required=True)
    predictfun.add_argument("--plans", required=True)

    polymarket = subparsers.add_parser("polymarket")
    polymarket.add_argument(
        "--state",
        action="append",
        required=True,
        help="Engine state JSON. Repeat for multiple accounts.",
    )

    args = parser.parse_args()
    risk_limits = _read_json(Path(args.risk_limits)) if args.risk_limits else None
    if args.venue == "predictfun":
        snapshot = export_predictfun_snapshot(
            intents_state=_read_json(Path(args.intents)),
            actual_state=_read_json(Path(args.actual)),
            plans_state=_read_json(Path(args.plans)),
            risk_limits=risk_limits,
        )
    else:
        snapshot = export_polymarket_snapshot(
            engine_states=[_read_json(Path(path)) for path in args.state],
            risk_limits=risk_limits,
        )

    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    temporary.replace(output)
    print(
        f"exported {args.venue} shadow snapshot: "
        f"desired={len(snapshot['desired'])} actual={len(snapshot['actual'])} "
        f"books={len(snapshot['books'])} output={output}"
    )
    return 0


def _read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.resolve().read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return data


if __name__ == "__main__":
    raise SystemExit(main())
