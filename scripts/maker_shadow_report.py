#!/usr/bin/env python3
"""Summarize long-running Python/Rust maker shadow parity."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.maker_shadow_collection import summary  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Report maker shadow difference rates.")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--venue", choices=("predictfun", "polymarket"))
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()

    report = summary(args.database.resolve(), venue=args.venue)
    if args.json_output:
        output = args.json_output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
        temporary.replace(output)

    if not report["venues"]:
        print("No unique shadow samples recorded yet.")
        return 0
    for row in report["venues"]:
        print(
            f"{row['venue']}: samples={row['samples']} fresh={row['fresh_samples']} "
            f"difference={_percent(row['difference_rate'])} "
            f"fresh_difference={_percent(row['fresh_difference_rate'])} "
            f"safety_mismatches={row['safety_mismatches']} "
            f"action_mismatches={row['action_mismatches']} "
            f"errors={row['errors']}"
        )
    for status in report["status"]:
        if status.get("last_error"):
            print(
                f"WARNING {status['venue']}: last_error={status['last_error']}",
                file=sys.stderr,
            )
    return 0


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.4f}%"


if __name__ == "__main__":
    raise SystemExit(main())
