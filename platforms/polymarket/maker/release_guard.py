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
    artifact_path: Path,
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[dict]:
    env = environ if environ is not None else os.environ
    if not _truthy(str(env.get("POLYMARKET_REQUIRE_RELEASE", ""))):
        return None

    expected_sha = str(env.get("POLYMARKET_RELEASE_SHA", "")).strip()
    if len(expected_sha) != 40:
        raise RuntimeError("POLYMARKET_RELEASE_SHA must be a full 40-character commit")

    resolved_artifact = artifact_path.resolve()
    release_root = resolved_artifact.parents[3]
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

    try:
        relative_artifact = resolved_artifact.relative_to(release_root).as_posix()
    except ValueError as exc:
        raise RuntimeError("release artifact escaped release directory") from exc

    artifact_hashes = manifest.get("artifacts_sha256")
    expected_hash = (
        artifact_hashes.get(relative_artifact)
        if isinstance(artifact_hashes, dict)
        else None
    )
    engine_artifact = "platforms/polymarket/maker/engine.py"
    if expected_hash is None and relative_artifact == engine_artifact:
        # Keep existing immutable releases valid for their engine entrypoint.
        expected_hash = manifest.get("engine_sha256")
    if not isinstance(expected_hash, str):
        raise RuntimeError("release artifact is not authorized by manifest")

    actual_hash = hashlib.sha256(resolved_artifact.read_bytes()).hexdigest()
    if expected_hash != actual_hash:
        message = (
            "release engine hash mismatch"
            if relative_artifact == engine_artifact
            else "release artifact hash mismatch"
        )
        raise RuntimeError(message)
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
