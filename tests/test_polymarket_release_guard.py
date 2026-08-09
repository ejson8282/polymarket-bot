import hashlib
import json
from pathlib import Path
import sys

import pytest


MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from release_guard import main, verify_release  # noqa: E402


SHA = "a" * 40


def _release(tmp_path: Path):
    root = tmp_path / SHA
    engine = root / "platforms" / "polymarket" / "maker" / "engine.py"
    engine.parent.mkdir(parents=True)
    engine.write_text("print('maker')\n", encoding="utf-8")
    proxy = engine.with_name("aggressive_proxy.py")
    proxy.write_text("print('proxy')\n", encoding="utf-8")
    manifest = {
        "source_repository": "ejson8282/polymarket-bot",
        "commit": SHA,
        "engine_sha256": hashlib.sha256(engine.read_bytes()).hexdigest(),
        "artifacts_sha256": {
            "platforms/polymarket/maker/engine.py": hashlib.sha256(
                engine.read_bytes()
            ).hexdigest(),
            "platforms/polymarket/maker/aggressive_proxy.py": hashlib.sha256(
                proxy.read_bytes()
            ).hexdigest(),
        },
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


def test_release_guard_accepts_manifested_aggressive_proxy(tmp_path):
    engine, manifest = _release(tmp_path)
    proxy = engine.with_name("aggressive_proxy.py")

    assert verify_release(
        proxy,
        {
            "POLYMARKET_REQUIRE_RELEASE": "1",
            "POLYMARKET_RELEASE_SHA": SHA,
        },
    ) == manifest


def test_release_guard_rejects_modified_aggressive_proxy(tmp_path):
    engine, _manifest = _release(tmp_path)
    proxy = engine.with_name("aggressive_proxy.py")
    proxy.write_text("print('modified')\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="artifact hash mismatch"):
        verify_release(
            proxy,
            {
                "POLYMARKET_REQUIRE_RELEASE": "1",
                "POLYMARKET_RELEASE_SHA": SHA,
            },
        )


def test_release_guard_rejects_unlisted_release_artifact(tmp_path):
    engine, _manifest = _release(tmp_path)
    unlisted = engine.with_name("unlisted.py")
    unlisted.write_text("print('unlisted')\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="not authorized"):
        verify_release(
            unlisted,
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


def test_release_guard_cli_requires_and_verifies_release(tmp_path, capsys):
    engine, _manifest = _release(tmp_path)

    assert main(
        [str(engine)],
        {
            "POLYMARKET_REQUIRE_RELEASE": "1",
            "POLYMARKET_RELEASE_SHA": SHA,
        },
    ) == 0
    assert capsys.readouterr().out.strip() == f"verified {SHA}"


def test_release_guard_cli_rejects_disabled_verification(tmp_path):
    engine, _manifest = _release(tmp_path)

    with pytest.raises(RuntimeError, match="must be required"):
        main([str(engine)], {})
