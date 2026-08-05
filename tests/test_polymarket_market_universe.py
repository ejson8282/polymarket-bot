import json
import sys
from pathlib import Path

import pytest

from platforms.polymarket.maker.account_roster import market_universe_sha256
from platforms.polymarket.maker.market_universe import (
    apply_market_universe,
    build_market_universe,
    normalize_market_source,
)
from scripts.generate_configs import main as generate_configs_main


def _market(token: str, paired: str, **extra) -> dict:
    return {
        "token_id": token,
        "paired_token_id": paired,
        "side": "YES",
        "enabled": True,
        **extra,
    }


def test_exact_duplicates_require_explicit_dedupe() -> None:
    row = _market("1", "2")
    source = {"markets": [row, dict(row)]}

    with pytest.raises(ValueError, match="exact duplicate"):
        normalize_market_source(source, source="vps2")

    normalized, removed = normalize_market_source(
        source,
        source="vps2",
        dedupe_exact=True,
    )
    assert normalized["markets"] == [row]
    assert removed == 1


def test_transient_activation_fields_do_not_enter_canonical_universe() -> None:
    normalized, _ = normalize_market_source(
        {
            "markets": [
                _market(
                    "1",
                    "2",
                    pending_activation=True,
                    pending_command_id="command-1",
                )
            ]
        },
        source="vps1",
    )

    assert "pending_activation" not in normalized["markets"][0]
    assert "pending_command_id" not in normalized["markets"][0]


def test_conflicting_rows_require_a_reviewed_preferred_source() -> None:
    vps1 = {"markets": [_market("1", "2", quote_size=50)]}
    vps2 = {"markets": [_market("1", "2", quote_size=100, question="Q")]}

    with pytest.raises(ValueError, match="set --prefer-source"):
        build_market_universe([("vps1", vps1), ("vps2", vps2)])

    result = build_market_universe(
        [("vps1", vps1), ("vps2", vps2)],
        prefer_source="vps2",
    )
    assert result.payload["markets"][0]["quote_size"] == 100
    assert result.payload["markets"][0]["question"] == "Q"
    assert result.conflicts_resolved == 1
    assert result.conflicts_disabled == 0

    disabled = build_market_universe(
        [("vps1", vps1), ("vps2", vps2)],
        prefer_source="vps2",
        disable_conflicts=True,
    )
    assert disabled.payload["markets"][0]["enabled"] is False
    assert disabled.conflicts_disabled == 1


def test_identity_and_day_night_conflicts_always_fail() -> None:
    with pytest.raises(ValueError, match="identity conflicts"):
        build_market_universe(
            [
                ("vps1", {"markets": [_market("1", "2", side="YES")]}),
                ("vps2", {"markets": [_market("1", "2", side="NO")]}),
            ],
            prefer_source="vps2",
        )

    with pytest.raises(ValueError, match="day in vps1 and night in vps2"):
        build_market_universe(
            [
                ("vps1", {"markets": [_market("1", "2")]}),
                ("vps2", {"night_markets": [_market("1", "2")]}),
            ],
            prefer_source="vps2",
        )


def test_union_is_deterministic_and_can_replace_host_markets() -> None:
    sources = [
        ("vps1", {"markets": [_market("3", "4")]}),
        ("vps2", {"markets": [_market("1", "2")]}),
    ]
    result = build_market_universe(sources)
    reversed_result = build_market_universe(list(reversed(sources)))
    applied = apply_market_universe(
        {"account": {"funder": "public"}, "markets": []},
        result.payload,
    )

    assert [row["token_id"] for row in applied["markets"]] == ["1", "3"]
    assert applied["account"] == {"funder": "public"}
    assert market_universe_sha256(applied) == market_universe_sha256(result.payload)
    assert reversed_result.payload == result.payload


def test_market_universe_output_is_non_secret(tmp_path: Path) -> None:
    result = build_market_universe(
        [("vps1", {"markets": [_market("1", "2")]})]
    )
    output = tmp_path / "universe.json"
    output.write_text(json.dumps(result.payload), encoding="utf-8")

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert set(payload) == {"schema_version", "markets", "night_markets", "build"}
    assert "account" not in payload


def test_multi_host_config_generation_requires_canonical_universe(
    monkeypatch,
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.json"
    roster = tmp_path / "roster.json"
    base.write_text(
        json.dumps({"account": {}, "markets": [_market("old", "pair")]}),
        encoding="utf-8",
    )
    roster.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "accounts": [
                    {
                        "account_index": 1,
                        "host_id": "vps1",
                        "funder": "0x" + "1" * 40,
                        "clash_port": 7901,
                    },
                    {
                        "account_index": 2,
                        "host_id": "vps2",
                        "funder": "0x" + "2" * 40,
                        "clash_port": 7901,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_configs.py",
            "--base",
            str(base),
            "--roster",
            str(roster),
            "--host-id",
            "vps1",
            "--dry-run",
        ],
    )

    with pytest.raises(SystemExit, match="--market-universe is required"):
        generate_configs_main()

    universe = tmp_path / "universe.json"
    universe.write_text(
        json.dumps({"markets": [_market("1", "2")], "night_markets": []}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_configs.py",
            "--base",
            str(base),
            "--roster",
            str(roster),
            "--market-universe",
            str(universe),
            "--host-id",
            "vps1",
            "--dry-run",
        ],
    )
    generate_configs_main()
