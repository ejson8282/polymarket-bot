from copy import deepcopy
from datetime import datetime, timezone

import pytest

from platforms.predictfun.maker.recovery_plan import assess_recovery, EXCHANGES


def inputs():
    row = {"account_id": "account_01", "idempotency_key": "old:g2",
           "market_id": 42, "is_neg_risk": False, "is_yield_bearing": True}
    return {
        "managed_state": {"pending_submissions": [row], "orders": [{"order_id": "keep"}],
                          "submission_generations": {"account_01": {"old": 1}}},
        "account_id": "account_01", "keys": ["old:g2"],
        "now": datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc),
        "fence": {"account_id": "account_01", "keys": ["old:g2"], "enforced": True,
                  "strict_ledger": True, "all_writers_quiesced": True,
                  "enforced_at": "2026-09-10T11:00:00Z", "verified_at": "2026-09-10T11:59:55Z"},
        "baseline": {"account_id": "account_01", "ok": True, "pagination_complete": True,
                     "open_orders": [], "positions": [], "maker": "0x" + "1" * 40,
                     "started_at": "2026-09-10T11:59:51Z",
                     "observed_at": "2026-09-10T11:59:56Z"},
        "nonce_evidence": [{**row, "chain_id": 56, "old_nonce_upper_bound": 0,
                            "current_nonce": 1, "old_nonce_valid": False,
                            "current_nonce_valid": True, "receipt_success": True,
                            "confirmations": 12, "maker": "0x" + "1" * 40,
                            "exchange": EXCHANGES[(False, True)],
                            "signing_audit_sha256": "a" * 64, "tx_hash": "0x" + "b" * 64,
                            "nonce_event_verified": True, "block_number": 111,
                            "receipt_block_number": 100, "block_hash": "0x" + "c" * 64,
                            "receipt_block_hash": "0x" + "d" * 64,
                            "mined_at": "2026-09-10T11:59:10Z",
                            "confirmed_at": "2026-09-10T11:59:30Z",
                            "observed_at": "2026-09-10T11:59:50Z"}],
    }


def test_review_plan_never_mutates_or_authorizes_runtime():
    args = inputs()
    before = deepcopy(args)
    result = assess_recovery(**args)
    assert args == before
    assert result["status"] == "ready_for_independent_review"
    assert result["activation_allowed"] is False
    assert result["runtime_write_allowed"] is False
    assert result["historical_submission_outcome"] == "unknown"
    result["archive_candidates"][0]["account_id"] = "changed"
    assert args == before


@pytest.mark.parametrize("field,value", [
    ("current_nonce", 0), ("current_nonce", True), ("old_nonce_upper_bound", None),
    ("old_nonce_valid", True), ("current_nonce_valid", False),
    ("receipt_success", False), ("confirmations", 1), ("chain_id", 137),
    ("nonce_event_verified", False), ("receipt_block_hash", ""),
    ("block_hash", ""), ("block_number", 112),
    ("mined_at", "2026-09-10T10:59:59Z"),
    ("maker", "0x" + "2" * 40), ("exchange", EXCHANGES[(True, True)]),
    ("is_neg_risk", True), ("is_yield_bearing", None), ("market_id", 43),
    ("signing_audit_sha256", ""), ("tx_hash", ""),
    ("observed_at", "2026-09-10T11:50:00Z"),
    ("observed_at", "2026-09-10T12:00:01Z"),
    ("confirmed_at", "2026-09-10T10:59:59Z"),
    ("confirmed_at", "2026-09-10T11:59:59Z"),
])
def test_unverified_nonce_barrier_blocks(field, value):
    args = inputs()
    args["nonce_evidence"][0][field] = value
    assert assess_recovery(**args)["status"] == "blocked"


@pytest.mark.parametrize("field,value", [
    ("positions", [{}]), ("open_orders", [{}]), ("pagination_complete", False),
    ("ok", False), ("account_id", "account_02"), ("maker", ""),
    ("observed_at", "2026-09-10T11:58:00Z"),
    ("observed_at", "2026-09-10T11:59:00Z"),
    ("started_at", "2026-09-10T11:59:40Z"),
])
def test_incomplete_or_pre_barrier_baseline_blocks(field, value):
    args = inputs()
    args["baseline"][field] = value
    assert assess_recovery(**args)["status"] == "blocked"


@pytest.mark.parametrize("field,value", [
    ("enforced", False), ("strict_ledger", False), ("all_writers_quiesced", False),
    ("account_id", "account_02"), ("keys", ["old"]), ("verified_at", "unknown"),
])
def test_fence_required(field, value):
    args = inputs()
    args["fence"][field] = value
    assert assess_recovery(**args)["status"] == "blocked"


def test_age_or_absence_never_proves_expiration():
    args = inputs()
    args["managed_state"]["pending_submissions"][0]["created_at"] = "2020-01-01T00:00:00Z"
    args["nonce_evidence"] = []
    assert assess_recovery(**args)["status"] == "blocked"


def test_exact_generation_and_account_allowlist():
    args = inputs()
    args["keys"] = ["old"]
    with pytest.raises(ValueError, match="allowlist"):
        assess_recovery(**args)
    args = inputs()
    args["managed_state"]["pending_submissions"][0]["account_id"] = "account_02"
    with pytest.raises(ValueError, match="allowlist"):
        assess_recovery(**args)


def test_duplicate_evidence_blocks():
    args = inputs()
    args["nonce_evidence"] *= 2
    assert assess_recovery(**args)["status"] == "blocked"


def test_other_account_records_and_generations_preserved():
    args = inputs()
    args["managed_state"]["pending_submissions"].append(
        {"account_id": "account_02", "idempotency_key": "old:g2"})
    before = deepcopy(args["managed_state"])
    result = assess_recovery(**args)
    assert len(result["archive_candidates"]) == 1
    assert args["managed_state"] == before
