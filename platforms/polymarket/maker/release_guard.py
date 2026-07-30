"""Fail-closed verification for immutable production maker releases."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Mapping, Optional


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def verify_release(
    engine_path: Path,
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[dict]:
    env = environ if environ is not None else os.environ
    if not _truthy(str(env.get("POLYMARKET_REQUIRE_RELEASE", ""))):
        return None

    expected_sha = str(env.get("POLYMARKET_RELEASE_SHA", "")).strip()
    if len(expected_sha) != 40:
        raise RuntimeError("POLYMARKET_RELEASE_SHA must be a full 40-character commit")

    resolved_engine = engine_path.resolve()
    release_root = resolved_engine.parents[3]
    manifest_path = release_root / ".release-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"release manifest unavailable: {manifest_path}") from exc

    if manifest.get("source_repository") != "ejson8282/polymarket-bot":
        raise RuntimeError("release manifest repository mismatch")
    if manifest.get("commit") != expected_sha:
        raise RuntimeError("release manifest commit mismatch")
    if release_root.name != expected_sha:
        raise RuntimeError("release directory does not match expected commit")

    actual_hash = hashlib.sha256(resolved_engine.read_bytes()).hexdigest()
    if manifest.get("engine_sha256") != actual_hash:
        raise RuntimeError("release engine hash mismatch")
    return manifest


def main(
    argv: Optional[list[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        raise SystemExit("usage: release_guard.py /absolute/path/to/engine.py")
    manifest = verify_release(Path(args[0]), environ=environ)
    if manifest is None:
        raise RuntimeError("release verification must be required in production")
    print(f"verified {manifest['commit']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
