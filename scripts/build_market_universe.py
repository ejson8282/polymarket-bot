"""Build one reviewed market universe from existing non-secret config files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from platforms.polymarket.maker.account_roster import market_universe_sha256  # noqa: E402
from platforms.polymarket.maker.market_universe import (  # noqa: E402
    build_market_universe,
    load_json_object,
)


def _source(value: str) -> tuple[str, Path]:
    label, separator, raw_path = value.partition("=")
    if not separator or not label.strip() or not raw_path.strip():
        raise argparse.ArgumentTypeError("source must use LABEL=/path/to/config.json")
    return label.strip(), Path(raw_path).expanduser().resolve()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a canonical Polymarket market universe"
    )
    parser.add_argument(
        "--source",
        action="append",
        type=_source,
        required=True,
        help="Labeled config source; repeat as LABEL=/path/to/config.json",
    )
    parser.add_argument(
        "--prefer-source",
        default="",
        help="Reviewed source that wins non-identity row conflicts",
    )
    parser.add_argument(
        "--dedupe-exact",
        action="store_true",
        help="Remove byte-equivalent duplicate events while reporting the count",
    )
    parser.add_argument(
        "--disable-conflicts",
        action="store_true",
        help="Keep reviewed conflicts in the universe but mark them disabled",
    )
    parser.add_argument("--output", required=True, help="Output market-universe JSON")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report without writing",
    )
    args = parser.parse_args()

    try:
        sources = [
            (label, load_json_object(path))
            for label, path in args.source
        ]
        result = build_market_universe(
            sources,
            prefer_source=args.prefer_source,
            dedupe_exact=args.dedupe_exact,
            disable_conflicts=args.disable_conflicts,
        )
    except ValueError as exc:
        sys.exit(f"ERROR: {exc}")

    digest = market_universe_sha256(result.payload)
    print(
        f"Markets: {len(result.payload['markets'])} day / "
        f"{len(result.payload['night_markets'])} night"
    )
    print(f"Removed: {result.exact_duplicates_removed} exact duplicate(s)")
    print(f"Resolved: {result.conflicts_resolved} reviewed conflict(s)")
    print(f"Disabled: {result.conflicts_disabled} conflicting event(s)")
    print(f"SHA256:  {digest}")
    if args.dry_run:
        print("Dry-run: no file written")
        return

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result.payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote:   {output_path}")


if __name__ == "__main__":
    main()
