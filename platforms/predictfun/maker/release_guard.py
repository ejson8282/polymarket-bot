"""Fail-closed verification for immutable Predict.fun dry-run releases."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Mapping, Optional


FULL_SHA_RE = re.compile(r"[0-9a-f]{40}")
SOURCE_REPOSITORY = "ejson8282/polymarket-bot"
ARTIFACT = "predictfun-dryrun"
REQUIRED_FILES = {
    "platforms/predictfun/maker/release_guard.py",
    "platforms/predictfun/maker/runner.py",
    "platforms/predictfun/maker/config.mainnet.json",
    "deploy/systemd/predictfun-dryrun.service",
    "deploy/systemd/predictfun-dryrun.timer",
}


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_files(release_root: Path) -> set[str]:
    return {
        path.relative_to(release_root).as_posix()
        for path in release_root.rglob("*")
        if path.is_file() and path.name != ".release-manifest.json"
    }


def verify_release(
    release_path: Path,
    environ: Optional[Mapping[str, str]] = None,
) -> dict:
    env = environ if environ is not None else os.environ
    if not _truthy(str(env.get("PREDICTFUN_REQUIRE_RELEASE", ""))):
        raise RuntimeError("Predict.fun release verification must be required")

    expected_sha = str(env.get("PREDICTFUN_RELEASE_SHA", "")).strip().lower()
    if not FULL_SHA_RE.fullmatch(expected_sha):
        raise RuntimeError(
            "PREDICTFUN_RELEASE_SHA must be a full 40-character commit SHA"
        )

    release_root = release_path.resolve(strict=True)
    if release_root.name != expected_sha:
        raise RuntimeError("release directory does not match expected commit")
    if release_root.is_symlink():
        raise RuntimeError("resolved release root must not be a symlink")

    manifest_path = release_root / ".release-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"release manifest unavailable: {manifest_path}") from exc
    if not isinstance(manifest, dict):
        raise RuntimeError("release manifest must be a JSON object")
    if manifest.get("source_repository") != SOURCE_REPOSITORY:
        raise RuntimeError("release manifest repository mismatch")
    if manifest.get("artifact") != ARTIFACT:
        raise RuntimeError("release manifest artifact mismatch")
    if manifest.get("commit") != expected_sha:
        raise RuntimeError("release manifest commit mismatch")

    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise RuntimeError("release manifest files must be a non-empty object")
    manifest_names = {str(name) for name in files}
    if not REQUIRED_FILES.issubset(manifest_names):
        missing = sorted(REQUIRED_FILES - manifest_names)
        raise RuntimeError(f"release manifest missing required files: {missing}")
    if _manifest_files(release_root) != manifest_names:
        raise RuntimeError("release file set does not match manifest")

    directories = [release_root, *(
        path for path in release_root.rglob("*") if path.is_dir()
    )]
    for directory in directories:
        if directory.stat().st_mode & 0o222:
            relative = directory.relative_to(release_root).as_posix() or "."
            raise RuntimeError(f"release directory is writable: {relative}")

    for relative_name, expected_hash in sorted(files.items()):
        relative = Path(str(relative_name))
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError(f"unsafe release manifest path: {relative_name}")
        target = release_root / relative
        try:
            resolved = target.resolve(strict=True)
        except FileNotFoundError as exc:
            raise RuntimeError(f"release file missing: {relative_name}") from exc
        if release_root not in resolved.parents or not resolved.is_file():
            raise RuntimeError(f"release file escapes root: {relative_name}")
        if resolved.stat().st_mode & 0o222:
            raise RuntimeError(f"release file is writable: {relative_name}")
        if _sha256(resolved) != str(expected_hash):
            raise RuntimeError(f"release file hash mismatch: {relative_name}")
    if manifest_path.stat().st_mode & 0o222:
        raise RuntimeError("release manifest is writable")
    return manifest


def main(
    argv: Optional[list[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        raise SystemExit("usage: release_guard.py /absolute/path/to/release")
    manifest = verify_release(Path(args[0]), environ=environ)
    print(f"verified Predict.fun release {manifest['commit']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
