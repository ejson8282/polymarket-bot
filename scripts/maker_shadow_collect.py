#!/usr/bin/env python3
"""Continuously compare changed Python maker state with the Rust dry-run core."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.maker_shadow_collection import (  # noqa: E402
    last_fingerprint,
    record_comparison,
    record_error,
    record_unchanged_poll,
    source_fingerprint,
)
from core.maker_shadow_compare import compare_case  # noqa: E402
from core.maker_shadow_export import (  # noqa: E402
    export_polymarket_snapshot,
    export_predictfun_snapshot,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read existing maker JSON state and record Python/Rust shadow parity. "
            "Never signs, calls an exchange, or sends/cancels orders."
        )
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument(
        "--rust-bin",
        type=Path,
        default=ROOT / "rust-maker" / "target" / "debug" / "maker-dry-run",
    )
    parser.add_argument("--risk-limits", type=Path)
    parser.add_argument("--interval-seconds", type=float, default=15.0)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument(
        "--run-seconds",
        type=float,
        default=0.0,
        help="Stop a loop after this duration. Zero means no time limit.",
    )
    subparsers = parser.add_subparsers(dest="venue", required=True)

    predictfun = subparsers.add_parser("predictfun")
    predictfun.add_argument("--intents", type=Path, required=True)
    predictfun.add_argument("--actual", type=Path, required=True)
    predictfun.add_argument("--plans", type=Path, required=True)

    polymarket = subparsers.add_parser("polymarket")
    polymarket.add_argument("--state", type=Path, action="append", required=True)

    args = parser.parse_args()
    if args.interval_seconds < 1:
        parser.error("--interval-seconds must be at least 1")
    binary = args.rust_bin.resolve()
    if not binary.is_file():
        parser.error(
            f"Rust binary not found at {binary}. Run: cd rust-maker && "
            "cargo build -p maker-dry-run"
        )

    database = args.database.resolve()
    snapshot_dir = args.snapshot_dir.resolve()
    risk_limits = _read_json(args.risk_limits) if args.risk_limits else None
    started = time.monotonic()
    while True:
        ok = _collect_once(
            args=args,
            database=database,
            snapshot_dir=snapshot_dir,
            binary=binary,
            risk_limits=risk_limits,
        )
        if not args.loop:
            return 0 if ok else 1
        if args.run_seconds > 0 and time.monotonic() - started >= args.run_seconds:
            return 0
        try:
            time.sleep(args.interval_seconds)
        except KeyboardInterrupt:
            return 0


def _collect_once(
    *,
    args: argparse.Namespace,
    database: Path,
    snapshot_dir: Path,
    binary: Path,
    risk_limits: dict[str, Any] | None,
) -> bool:
    venue = str(args.venue)
    try:
        payloads, states = _source_states(args)
        fingerprint = source_fingerprint(payloads)
        if fingerprint == last_fingerprint(database, venue):
            record_unchanged_poll(
                database=database,
                venue=venue,
                fingerprint=fingerprint,
            )
            print(f"UNCHANGED {venue} {fingerprint[:12]}", flush=True)
            return True

        if venue == "predictfun":
            snapshot = export_predictfun_snapshot(
                intents_state=states["intents"],
                actual_state=states["actual"],
                plans_state=states["plans"],
                risk_limits=risk_limits,
            )
        else:
            snapshot = export_polymarket_snapshot(
                engine_states=states["engine_states"],
                risk_limits=risk_limits,
            )

        snapshot_path = _snapshot_path(snapshot_dir, venue, fingerprint)
        _write_json(snapshot_path, snapshot)
        comparison = compare_case(binary, snapshot_path)
        inserted = record_comparison(
            database=database,
            venue=venue,
            fingerprint=fingerprint,
            snapshot=snapshot,
            comparison=comparison,
            snapshot_path=snapshot_path,
        )
        status = "MATCH" if comparison["matched"] else "MISMATCH"
        sample_status = "NEW" if inserted else "DUPLICATE"
        print(
            f"{status} {sample_status} {venue} {fingerprint[:12]} "
            f"desired={len(snapshot.get('desired') or [])} "
            f"actual={len(snapshot.get('actual') or [])}",
            flush=True,
        )
        return True
    except Exception as error:
        record_error(database=database, venue=venue, error=str(error))
        print(f"ERROR {venue} {error}", file=sys.stderr, flush=True)
        return False


def _source_states(
    args: argparse.Namespace,
) -> tuple[list[tuple[str, bytes]], dict[str, Any]]:
    if args.venue == "predictfun":
        paths = {
            "intents": args.intents.resolve(),
            "actual": args.actual.resolve(),
            "plans": args.plans.resolve(),
        }
        payloads = [(name, path.read_bytes()) for name, path in paths.items()]
        states = {
            name: _decode_json(payload, paths[name])
            for name, payload in payloads
        }
        return payloads, states

    paths = [path.resolve() for path in args.state]
    payloads = [
        (f"state-{index}:{path.name}", path.read_bytes())
        for index, path in enumerate(paths, start=1)
    ]
    return payloads, {
        "engine_states": [
            _decode_json(payload, path)
            for (_, payload), path in zip(payloads, paths)
        ]
    }


def _snapshot_path(directory: Path, venue: str, fingerprint: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return directory / venue / f"{timestamp}-{fingerprint[:16]}.json"


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    return _decode_json(path.resolve().read_bytes(), path)


def _decode_json(payload: bytes, path: Path) -> dict[str, Any]:
    value = json.loads(payload.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
