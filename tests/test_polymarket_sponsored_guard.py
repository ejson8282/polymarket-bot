from pathlib import Path
import sys
import time


MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from sponsored_guard import SponsoredRiskGuard  # noqa: E402


CID = "0x" + "1" * 64
CID_2 = "0x" + "2" * 64


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _official(*, sponsored=80.0, native=20.0, sponsors=1):
    return {
        "condition_id": CID,
        "sponsored_daily_rate": sponsored,
        "native_daily_rate": native,
        "total_daily_rate": sponsored + native,
        "sponsors_count": sponsors,
    }


def _ready_guard(config=None):
    guard = SponsoredRiskGuard(config)
    guard._official = {CID: _official()}
    guard._official_last_success_at = time.time()
    guard._official_ok = True
    return guard


def test_single_sponsor_concentration_blocks_admission_and_reduces_runtime_size():
    guard = _ready_guard()

    admission = guard.assess(CID, for_admission=True)
    runtime = guard.assess(CID)

    assert admission["status"] == "blocked"
    assert admission["size_cap"] == 0.0
    assert "single_sponsor_dependency" in admission["reasons"]
    assert runtime["status"] == "caution"
    assert runtime["size_cap"] == 0.25
    assert runtime["sponsor_ratio"] == 0.8


def test_official_reward_drop_latches_a_runtime_block():
    guard = _ready_guard({"reward_drop_cancel_pct": 0.20, "cooldown_sec": 600})
    now = time.time()

    guard._detect_official_changes(
        {CID: _official(sponsored=100, native=20)},
        {CID: _official(sponsored=70, native=20)},
        now,
    )
    assessment = guard.assess(CID, now=now + 1)

    assert assessment["status"] == "blocked"
    assert assessment["size_cap"] == 0.0
    assert "official_reward_drop_30%" in assessment["reasons"]
    assert assessment["block_until"] >= now + 600


def test_betmoar_cancelled_sponsorship_blocks_until_reward_flow_ends():
    guard = _ready_guard({"cooldown_sec": 600})
    now = time.time()
    reward_end = now + 3600

    guard._apply_betmoar_blocks(
        {
            CID: {
                "is_cancelled": True,
                "next_end_at": reward_end,
                "cancelled_daily_rate": 80,
                "sponsorships": [],
            }
        },
        {},
        now,
    )
    assessment = guard.assess(CID, now=now + 1)

    assert assessment["status"] == "blocked"
    assert "betmoar_sponsor_cancelled_80%" in assessment["reasons"]
    assert assessment["block_until"] >= reward_end


def test_small_cancelled_sponsor_does_not_block_the_market():
    guard = _ready_guard({"betmoar_cancel_min_total_ratio": 0.20})
    now = time.time()

    guard._apply_betmoar_blocks(
        {
            CID: {
                "is_cancelled": True,
                "next_end_at": now + 3600,
                "cancelled_daily_rate": 2,
                "sponsorships": [],
            }
        },
        {},
        now,
    )

    assessment = guard.assess(CID, now=now + 1)
    assert assessment["status"] == "caution"
    assert not any(
        reason.startswith("betmoar_sponsor_cancelled_")
        for reason in assessment["reasons"]
    )


def test_transient_official_staleness_only_reduces_existing_quotes():
    guard = _ready_guard({"source_stale_reduce_after_sec": 60})
    guard._official_last_success_at = time.time() - 120

    runtime = guard.assess(CID)
    admission = guard.assess(CID, for_admission=True)

    assert runtime["status"] == "caution"
    assert runtime["size_cap"] == 0.25
    assert "official_source_stale" in runtime["reasons"]
    assert admission["status"] == "blocked"


def test_state_payload_counts_only_relevant_engine_markets():
    guard = _ready_guard()
    safe_cid = CID_2
    guard._official[safe_cid] = {
        "condition_id": safe_cid,
        "sponsored_daily_rate": 5,
        "native_daily_rate": 95,
        "total_daily_rate": 100,
        "sponsors_count": 1,
    }

    payload = guard.state_payload(
        {
            CID: guard.assess(CID),
            safe_cid: guard.assess(safe_cid),
        }
    )

    assert payload["status"] == "caution"
    assert payload["counts"] == {
        "safe": 1,
        "caution": 1,
        "blocked": 0,
        "unknown": 0,
    }


def test_official_fetch_paginates_and_normalizes_rates():
    cursors = []

    def fake_get(_url, *, params, timeout, proxies):
        assert timeout == 12
        assert proxies is None
        cursors.append(params.get("next_cursor", ""))
        if "next_cursor" not in params:
            return _Response({
                "data": [{
                    "condition_id": CID,
                    "sponsored_daily_rate": 80,
                    "native_daily_rate": 20,
                    "total_daily_rate": 100,
                    "sponsors_count": 1,
                }],
                "next_cursor": "page-2",
            })
        return _Response({
            "data": [{
                "condition_id": CID_2,
                "sponsored_daily_rate": 5,
                "native_daily_rate": 95,
                "total_daily_rate": 100,
                "sponsors_count": 2,
            }],
            "next_cursor": "LTE=",
        })

    guard = SponsoredRiskGuard(request_get=fake_get)
    result = guard._fetch_official(None)

    assert cursors == ["", "page-2"]
    assert result[CID]["sponsored_daily_rate"] == 80
    assert result[CID_2]["sponsors_count"] == 2


def test_betmoar_fetch_extracts_cancel_and_early_withdrawal():
    now = time.time()

    def fake_get(_url, *, timeout, proxies):
        assert timeout == 12
        assert proxies is None
        return _Response({
            "active": [{
                "market_id": CID,
                "sponsor": "0xabc",
                "market_question": "Example?",
                "is_cancelled": True,
                "rate_per_minute_usdc": 0.05,
                "withdrawn_at": datetime_from_ts(now),
                "rewards_end_at": datetime_from_ts(now + 3600),
            }],
            "recentWithdrawals": [{
                "market_id": CID,
                "sponsor": "0xabc",
                "market_question": "Example?",
                "is_early_withdraw": True,
                "block_timestamp": datetime_from_ts(now),
            }],
        })

    guard = SponsoredRiskGuard(request_get=fake_get)
    active, early = guard._fetch_betmoar(None)

    assert active[CID]["is_cancelled"] is True
    assert active[CID]["sponsors"] == ["0xabc"]
    assert active[CID]["cancelled_daily_rate"] == 72
    assert active[CID]["sponsorships"][0]["daily_rate"] == 72
    assert active[CID]["next_end_at"] >= now + 3599
    assert early[CID]["sponsor"] == "0xabc"


def datetime_from_ts(value):
    from datetime import datetime, timezone

    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
