from concurrent.futures import ThreadPoolExecutor
import sqlite3

import pytest

from platforms.polymarket.maker.exit_timeout_notices import claim_exit_timeout_notice


def test_one_claim_survives_reopen_and_has_no_time_based_expiry(tmp_path):
    path = tmp_path / "notices.sqlite3"
    assert claim_exit_timeout_notice(path, 2, "101", "sell-1")
    for _ in range(4):
        assert not claim_exit_timeout_notice(path, 2, "101", "sell-1")
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM exit_timeout_notices").fetchone()[0] == 1


def test_other_accounts_tokens_and_orders_are_not_muted(tmp_path):
    path = tmp_path / "notices.sqlite3"
    for identity in ((1, "101", "sell-1"), (2, "101", "sell-1"),
                     (1, "102", "sell-1"), (1, "101", "sell-2")):
        assert claim_exit_timeout_notice(path, *identity)
        assert not claim_exit_timeout_notice(path, *identity)


def test_concurrent_claims_only_have_one_winner(tmp_path):
    path = tmp_path / "notices.sqlite3"
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: claim_exit_timeout_notice(path, 1, "101", "sell-1"), range(8)))
    assert sum(results) == 1


@pytest.mark.parametrize("identity", [(-1, "101", "sell-1"), (True, "101", "sell-1"),
                                      (1, "", "sell-1"), (1, "101", " ")])
def test_invalid_identity_never_claims(tmp_path, identity):
    path = tmp_path / "notices.sqlite3"
    with pytest.raises(ValueError):
        claim_exit_timeout_notice(path, *identity)
    assert not path.exists()


def test_corrupt_store_does_not_reset_deduplication(tmp_path):
    path = tmp_path / "notices.sqlite3"
    path.write_bytes(b"corrupt notice store")
    with pytest.raises(sqlite3.DatabaseError):
        claim_exit_timeout_notice(path, 1, "101", "sell-1")
    assert path.read_bytes() == b"corrupt notice store"


def test_failed_locked_claim_can_retry_after_recovery(tmp_path):
    path = tmp_path / "notices.sqlite3"
    assert claim_exit_timeout_notice(path, 1, "101", "sell-1")
    with sqlite3.connect(path) as owner:
        owner.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError):
            claim_exit_timeout_notice(path, 1, "101", "sell-2")
    assert claim_exit_timeout_notice(path, 1, "101", "sell-2")
