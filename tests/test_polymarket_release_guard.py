import hashlib
import json
from pathlib import Path
import sys

import pytest


MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from release_guard import verify_release  # noqa: E402


SHA = "a" * 40


def _release(tmp_path: Path):
    root = tmp_path / SHA
    engine = root / "platforms" / "polymarket" / "maker" / "engine.py"
    engine.parent.mkdir(parents=True)
    engine.write_text("print('maker')\n", encoding="utf-8")
    manifest = {
        "source_repository": "ejson8282/polymarket-bot",
        "commit": SHA,
        "engine_sha256": hashlib.sha256(engine.read_bytes()).hexdigest(),
    }
    (root / ".release-manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    return engine, manifest


def test_release_guard_accepts_exact_immutable_release(tmp_path):
    engine, manifest = _release(tmp_path)

    assert verify_release(
        engine,
        {
            "POLYMARKET_REQUIRE_RELEASE": "1",
            "POLYMARKET_RELEASE_SHA": SHA,
        },
    ) == manifest


def test_release_guard_rejects_modified_engine(tmp_path):
    engine, _manifest = _release(tmp_path)
    engine.write_text("print('modified')\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="engine hash mismatch"):
        verify_release(
            engine,
            {
                "POLYMARKET_REQUIRE_RELEASE": "1",
                "POLYMARKET_RELEASE_SHA": SHA,
            },
        )


def test_release_guard_rejects_wrong_commit(tmp_path):
    engine, _manifest = _release(tmp_path)

    with pytest.raises(RuntimeError, match="commit mismatch"):
        verify_release(
            engine,
            {
                "POLYMARKET_REQUIRE_RELEASE": "1",
                "POLYMARKET_RELEASE_SHA": "b" * 40,
            },
        )
