from copy import deepcopy
from datetime import datetime, timezone
import json
import sys
from types import SimpleNamespace

import pytest

from platforms.predictfun.maker.recovery_evidence import (
    ProxyAccountReader, _NoRedirect, collect_account_baseline, collect_nonce_barrier, make_bsc_reader,
)
from platforms.predictfun.maker.recovery_plan import EXCHANGES, assess_recovery


NOW = 1_789_000_000
MAKER = "0x" + "1" * 40
EXCHANGE = EXCHANGES[(False, True)]
TX, HEAD, MINED = ("0x" + c * 64 for c in "abc")
PENDING = {"account_id": "account_01", "idempotency_key": "old:g2",
           "market_id": 42, "is_neg_risk": False, "is_yield_bearing": True}


def page(rows=None, cursor=None):
    return {"ok": True, "alias": "account_01", "status": 200,
            "response": {"success": True, "data": rows or [], "cursor": cursor}}


def balance():
    return {"ok": True, "alias": "account_01", "chain_id": 56,
            "owner": MAKER, "collateral": "USDT", "balance": "42.01"}


class Reads:
    def __init__(self):
        self.pages = {"orders": [page()], "positions": [page()]}
        self.balance = balance()
        self.calls = []

    def __call__(self, resource, query):
        self.calls.append((resource, query))
        return deepcopy(self.balance if resource == "allowances" else self.pages[resource].pop(0))


def baseline(reads, **kwargs):
    return collect_account_baseline(reads, account_id="account_01", maker=MAKER,
                                    clock=lambda: NOW + 1, **kwargs)


def test_complete_pages_preserve_nonempty_state_without_signatures():
    reads = Reads()
    reads.pages["orders"] = [page([{"id": "one", "order": {"signature": "DO_NOT_EXPORT"}}], "next"),
                             page([{"id": "two"}])]
    reads.pages["positions"] = [page([{"amount": "2"}])]
    result = baseline(reads)
    assert len(result["open_orders"]) == 2 and len(result["positions"]) == 1
    assert result["pagination_complete"] is True
    assert result["balance"] == "42.01"
    assert reads.calls[1] == ("orders", {"first": 100, "status": "OPEN", "after": "next"})
    assert "DO_NOT_EXPORT" not in json.dumps(result)


@pytest.mark.parametrize("bad", [None, {}, {"ok": False}, {"response": {}},
    {"ok": True, "alias": "account_02", "response": {"success": True, "data": [], "cursor": None}},
    {"ok": True, "alias": "account_01", "response": {"success": True, "data": []}},
    {"ok": True, "alias": "account_01", "response": {"success": False, "data": [], "cursor": None}},
    {"ok": True, "alias": "account_01", "response": {"success": True, "data": {}, "cursor": None}},
])
def test_unknown_pages_are_not_zero(bad):
    reads = Reads()
    reads.pages["orders"] = [bad]
    with pytest.raises(ValueError, match="account_page_invalid"):
        baseline(reads)


@pytest.mark.parametrize("row", [None, {}, [], "row"])
def test_malformed_nonempty_rows_are_not_ignored(row):
    reads = Reads()
    reads.pages["orders"] = [page([row])]
    with pytest.raises(ValueError, match="row_invalid"):
        baseline(reads)


def test_repeated_cursor_and_page_cap_block():
    reads = Reads()
    reads.pages["orders"] = [page(cursor="repeat"), page(cursor="repeat")]
    with pytest.raises(ValueError, match="cursor_invalid"):
        baseline(reads)
    reads = Reads()
    reads.pages["orders"] = [page(cursor="next")]
    with pytest.raises(ValueError, match="pagination_incomplete"):
        baseline(reads, max_pages=1)


@pytest.mark.parametrize("field,value", [("alias", "account_02"), ("owner", "0x" + "2" * 40),
    ("chain_id", 137), ("collateral", "USDC"), ("ok", False),
    ("balance", "NaN"), ("balance", "Infinity"), ("balance", "-1"), ("balance", None)])
def test_balance_must_match_account_and_be_known(field, value):
    reads = Reads()
    reads.balance[field] = value
    with pytest.raises(ValueError):
        baseline(reads)


def test_slow_account_snapshot_is_not_fresh():
    times = iter([NOW, NOW + 61])
    with pytest.raises(ValueError, match="too_slow"):
        collect_account_baseline(Reads(), account_id="account_01", maker=MAKER, clock=lambda: next(times))


class Chain:
    to_checksum_address = staticmethod(lambda address: address)

    def __init__(self):
        self.chain_id = 56
        self.eth = self
        self.head = {"number": 111, "timestamp": NOW - 1, "hash": HEAD}
        self.mined = {"number": 100, "timestamp": NOW - 20, "hash": MINED}
        self.receipt = {"status": 1, "transactionHash": TX, "blockNumber": 100, "blockHash": MINED}
        self.event = {"address": EXCHANGE, "transactionHash": TX, "blockHash": MINED,
                      "args": {"user": MAKER, "newNonce": 1}}
        self.current = 1
        self.old_valid = False
        self.new_valid = True
        self.reorg = False
        self.calls = []

    def get_block(self, block):
        if block == "latest":
            return deepcopy(self.head)
        if block == 100:
            return deepcopy(self.mined)
        return {**self.head, "hash": "0x" + "d" * 64} if self.reorg else deepcopy(self.head)

    def get_transaction_receipt(self, tx):
        assert tx == TX
        return deepcopy(self.receipt)

    def contract(self, *, address, abi):
        assert address == EXCHANGE
        assert all(row.get("stateMutability") == "view" for row in abi if row["type"] == "function")
        def call(name, owner, value=None):
            assert owner == MAKER
            def read(**kwargs):
                self.calls.append((name, value, kwargs))
                return self.current if name == "nonces" else (self.new_valid if value == self.current else self.old_valid)
            return SimpleNamespace(call=read)
        return SimpleNamespace(
            functions=SimpleNamespace(nonces=lambda owner: call("nonces", owner),
                                      isValidNonce=lambda owner, value: call("isValidNonce", owner, value)),
            events=SimpleNamespace(NonceIncremented=lambda: SimpleNamespace(
                process_receipt=lambda receipt: [deepcopy(self.event)])))


def nonce(chain, **kwargs):
    return collect_nonce_barrier(chain, PENDING, account_id="account_01", maker=MAKER,
                                  tx_hash=TX, old_nonce_upper_bound=0,
                                  signing_audit_sha256="e" * 64, clock=lambda: NOW, **kwargs)


def test_receipt_event_nonce_reads_and_complete_baseline_feed_plan():
    chain = Chain()
    evidence = nonce(chain)
    assert evidence["nonce_event_verified"] is True
    assert evidence["confirmations"] == 12
    assert all(row[2] == {"block_identifier": 111} for row in chain.calls)
    result = assess_recovery({"pending_submissions": [PENDING]}, account_id="account_01",
        keys=["old:g2"], nonce_evidence=[evidence], baseline=baseline(Reads()),
        now=datetime.fromtimestamp(NOW + 2, timezone.utc),
        fence={"account_id": "account_01", "keys": ["old:g2"], "enforced": True,
               "strict_ledger": True, "all_writers_quiesced": True,
               "verified_at": datetime.fromtimestamp(NOW + 1, timezone.utc).isoformat(),
               "enforced_at": datetime.fromtimestamp(NOW - 100, timezone.utc).isoformat()})
    assert result["status"] == "ready_for_independent_review"
    assert result["activation_allowed"] is False
    assert result["runtime_write_allowed"] is False


@pytest.mark.parametrize("target,field,value", [
    ("receipt", "status", 0), ("receipt", "status", True),
    ("receipt", "transactionHash", "0x" + "f" * 64),
    ("receipt", "blockNumber", 101), ("receipt", "blockHash", "0x" + "f" * 64),
    ("head", "timestamp", NOW - 31), ("head", "timestamp", NOW + 6),
    ("mined", "number", 99), ("mined", "timestamp", NOW),
    ("event", "address", EXCHANGES[(True, True)]), ("event", "removed", True),
    ("event", "transactionHash", "0x" + "f" * 64),
    ("event", "blockHash", "0x" + "f" * 64),
])
def test_wrong_receipt_or_chain_context_blocks(target, field, value):
    chain = Chain()
    getattr(chain, target)[field] = value
    with pytest.raises(ValueError):
        nonce(chain)


@pytest.mark.parametrize("field,value", [("user", "0x" + "2" * 40), ("newNonce", 2), ("newNonce", True)])
def test_other_maker_or_nonce_event_is_not_evidence(field, value):
    chain = Chain()
    chain.event["args"][field] = value
    with pytest.raises(ValueError, match="event_not_bound"):
        nonce(chain)


@pytest.mark.parametrize("field,value", [("chain_id", 137), ("current", 0),
    ("old_valid", True), ("new_valid", False), ("reorg", True)])
def test_invalid_nonce_or_reorg_blocks(field, value):
    chain = Chain()
    setattr(chain, field, value)
    with pytest.raises(ValueError):
        nonce(chain)


def test_missing_audited_upper_bound_does_not_default_to_zero():
    with pytest.raises(ValueError, match="input_invalid"):
        collect_nonce_barrier(Chain(), PENDING, account_id="account_01", maker=MAKER,
            tx_hash=TX, old_nonce_upper_bound=None, signing_audit_sha256="e" * 64, clock=lambda: NOW)


@pytest.mark.parametrize("resource", ["submit-order", "cancel-orders", "submissions/old", "../orders"])
def test_client_refuses_mutating_or_recovering_routes(resource):
    client = ProxyAccountReader("http://127.0.0.1:8791", "account_01")
    with pytest.raises(ValueError, match="route_not_allowed"):
        client(resource, {})


def test_client_is_get_only_and_does_not_follow_redirects():
    client = ProxyAccountReader("http://127.0.0.1:8791", "account_01")
    class Response:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def read(self, limit):
            assert limit == 2_000_001
            return json.dumps(page()).encode()
    def open_request(request, timeout):
        assert request.get_method() == "GET" and request.data is None
        assert request.full_url.startswith("http://127.0.0.1:8791/predictfun/accounts/account_01/orders?")
        assert "Authorization" not in request.headers
        return Response()
    client.opener = SimpleNamespace(open=open_request)
    assert client("orders", {"first": 100})["ok"] is True
    with pytest.raises(ValueError, match="redirect_refused"):
        _NoRedirect().redirect_request(None, None, 302, "", {}, "http://elsewhere.invalid")


@pytest.mark.parametrize("version_name", ["ExtraDataToPOAMiddleware", "geth_poa_middleware"])
def test_bsc_client_injects_poa_and_bounds_network_timeout(monkeypatch, version_name):
    calls = []
    class Web3:
        @staticmethod
        def HTTPProvider(url, request_kwargs):
            assert request_kwargs == {"timeout": 10}
            return "provider"
        def __init__(self, provider):
            self.middleware_onion = SimpleNamespace(inject=lambda middleware, layer: calls.append((middleware, layer)))
    marker = object()
    monkeypatch.setitem(sys.modules, "web3", SimpleNamespace(Web3=Web3))
    monkeypatch.setitem(sys.modules, "web3.middleware", SimpleNamespace(**{version_name: marker}))
    make_bsc_reader("https://bsc.example.invalid")
    assert calls == [(marker, 0)]
