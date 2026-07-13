#!/usr/bin/env python3
"""Compare the Python reference oracle with the Rust dry-run core offline."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.maker_shadow_compare import compare_case


def _case_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(candidate for candidate in path.glob("*.json") if candidate.is_file())
    raise FileNotFoundError(f"case path does not exist: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Offline Python-vs-Rust maker-core shadow comparison."
    )
    parser.add_argument("cases", type=Path, help="JSON case file or directory")
    parser.add_argument(
        "--rust-bin",
        type=Path,
        default=ROOT / "rust-maker" / "target" / "debug" / "maker-dry-run",
    )
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    binary = args.rust_bin.resolve()
    if not binary.is_file():
        parser.error(
            f"Rust binary not found at {binary}. Run: cd rust-maker && "
            "cargo build -p maker-dry-run"
        )

    results = [compare_case(binary, path.resolve()) for path in _case_paths(args.cases.resolve())]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "offline_shadow",
        "summary": {
            "cases": len(results),
            "matched": sum(1 for result in results if result["matched"]),
            "mismatched": sum(1 for result in results if not result["matched"]),
        },
        "results": results,
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")

    for result in results:
        print(f"{'PASS' if result['matched'] else 'FAIL'}  {result['case']}")
    summary = report["summary"]
    print(
        f"shadow parity: {summary['matched']}/{summary['cases']} matched; "
        f"{summary['mismatched']} mismatched"
    )
    return 0 if summary["mismatched"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
