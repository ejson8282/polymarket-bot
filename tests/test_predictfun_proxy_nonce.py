import importlib.util
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest


@pytest.fixture
def proxy(tmp_path, monkeypatch):
    path = Path(__file__).resolve().parents[1] / "deploy/mac-mini/predictfun_api_proxy.py"
    spec = importlib.util.spec_from_file_location("proxy_nonce_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ORDER_LEDGER_FILE", tmp_path / "ledger.json")
    monkeypatch.setitem(sys.modules, "web3.middleware", SimpleNamespace(ExtraDataToPOAMiddleware="poa-test"))
    return module


def env(**settings):
    return {"PREDICTFUN_ACCOUNT_KEYS_JSON": json.dumps({
        "account_01": {"wallet_address": "0x" + "1" * 40, **settings},
        "account_02": {"wallet_address": "0x" + "2" * 40},
    })}


def body(key="old:g2"):
    return {"submit": True, "confirm": "SUBMIT_PREDICTFUN_ORDER",
            "idempotency_key": key, "market_id": 42, "side": "BUY",
            "price": "0.4", "size": "2", "token_id": "1",
            "is_post_only": True, "self_trade_prevention": "CANCEL_MAKER",
            "strategy": "LIMIT", "max_notional": "1.6"}


def fail(*args, **kwargs):
    raise AssertionError("must not sign or submit")


@pytest.mark.parametrize("contents", [None, "{", "[]", '{"orders":[]}',
                                     '{"orders":{"account_01:old:g2":null}}',
                                     '{"orders":{"account_01:old:g2":{"quarantined":"true"}}}'])
def test_strict_ledger_loss_blocks_before_signing(proxy, monkeypatch, contents):
    if contents is not None:
        proxy.ORDER_LEDGER_FILE.write_text(contents)
    monkeypatch.setattr(proxy, "_signed_order_payload", fail)
    monkeypatch.setattr(proxy, "_authenticated_request", fail)
    with pytest.raises(ValueError, match="order_ledger"):
        proxy.submit_order(env(require_order_ledger=True), "account_01", body())
    with pytest.raises(ValueError, match="order_ledger"):
        proxy.submission_status(env(require_order_ledger=True), "account_01", "old:g2")


def test_quarantined_key_never_signs_or_becomes_rejection(proxy, monkeypatch):
    ledger = {"version": 1, "orders": {"account_01:old:g2": {
        "quarantined": True, "ok": True, "status": "open", "order_hash": "0x" + "a" * 64}}}
    proxy.ORDER_LEDGER_FILE.write_text(json.dumps(ledger))
    before = proxy.ORDER_LEDGER_FILE.read_bytes()
    monkeypatch.setattr(proxy, "_signed_order_payload", fail)
    monkeypatch.setattr(proxy, "_authenticated_request", fail)
    for _ in range(3):
        assert proxy.submit_order(env(require_order_ledger=True), "account_01", body())["error"] == "submission_quarantined"
        result = proxy.submission_status(env(require_order_ledger=True), "account_01", "old:g2")
        assert result["found"] is True
        assert result["submission_state"] == "quarantined"
        assert result["order_status"] == "unknown"
    assert proxy.ORDER_LEDGER_FILE.read_bytes() == before
    assert proxy.submission_status(env(), "account_02", "old:g2")["found"] is False
    assert proxy.submission_status(env(), "account_01", "old")["found"] is False


def test_nonce_feature_requires_strict_ledger(proxy):
    with pytest.raises(ValueError, match="requires_strict"):
        proxy.submit_order(env(use_exchange_nonce=True), "account_01", body())


@pytest.mark.parametrize("name", ["use_exchange_nonce", "require_order_ledger"])
def test_string_flags_cannot_silently_disable_protections(proxy, name):
    with pytest.raises(ValueError, match="must_be_boolean"):
        proxy.submit_order(env(**{name: "true"}), "account_01", body())


def test_unknown_legacy_nonce_requires_audit_not_zero_assumption(proxy, monkeypatch):
    request = body()
    proxy.ORDER_LEDGER_FILE.write_text(json.dumps({"orders": {"account_01:old:g2": {
        "ok": False, "request_fingerprint": proxy._order_request_fingerprint(request)}}}))
    monkeypatch.setattr(proxy, "_signed_order_payload", fail)
    with pytest.raises(ValueError, match="legacy_submission_nonce_unknown"):
        proxy.submit_order(env(require_order_ledger=True, use_exchange_nonce=True), "account_01", request)


@pytest.mark.parametrize("flags", [(False, False), (True, False), (False, True), (True, True)])
def test_nonce_bound_to_exact_maker_and_exchange(proxy, monkeypatch, flags):
    calls = []
    def read(environment, exchange, maker):
        calls.append((exchange, maker))
        return 7
    monkeypatch.setattr(proxy, "_read_exchange_nonce", read)
    order = {"maker": "smart-account", "nonce": "0"}
    proxy._bind_exchange_nonce({}, order, {}, {"isNegRisk": flags[0], "isYieldBearing": flags[1]})
    assert order["nonce"] == "7"
    assert calls == [(proxy.PREDICT_EXCHANGES[flags], "smart-account")]


@pytest.mark.parametrize("requested", [0, "0", None, -1, True, "7.0", 2**256])
def test_explicit_old_or_invalid_nonce_not_silently_replaced(proxy, monkeypatch, requested):
    monkeypatch.setattr(proxy, "_read_exchange_nonce", lambda *a: 7)
    with pytest.raises(ValueError, match="nonce"):
        proxy._bind_exchange_nonce({}, {"maker": "owner"}, {"nonce": requested},
                                   {"isNegRisk": False, "isYieldBearing": True})


def test_unknown_key_retains_original_nonce_on_retry(proxy, monkeypatch):
    request = body()
    proxy.ORDER_LEDGER_FILE.write_text(json.dumps({"orders": {"account_01:old:g2": {
        "ok": False, "request_fingerprint": proxy._order_request_fingerprint(request),
        "order_nonce": "3", "order_expiration": "9999999999"}}}))
    def signed(environment, alias, request):
        assert request["nonce"] == "3"
        assert request["expiration"] == 9999999999
        raise ValueError("order_nonce_not_current")
    monkeypatch.setattr(proxy, "_signed_order_payload", signed)
    monkeypatch.setattr(proxy, "_authenticated_request", fail)
    with pytest.raises(ValueError, match="not_current"):
        proxy.submit_order(env(require_order_ledger=True), "account_01", request)


@pytest.mark.parametrize("chain,nonce,current_valid,old_valid", [
    (56, 1, True, False), (56, 0, True, False), (137, 1, True, False),
    (56, 1, False, False), (56, 1, True, True), (56, -1, True, False),
])
def test_rpc_chain_and_same_block_validation(proxy, monkeypatch, chain, nonce, current_valid, old_valid):
    calls = []
    class Call:
        def __init__(self, value):
            self.value = value
        def call(self, **kwargs):
            calls.append(kwargs)
            return self.value
    funcs = SimpleNamespace(nonces=lambda owner: Call(nonce),
                            isValidNonce=lambda owner, n: Call(current_valid if n == nonce else old_valid))
    class Web3:
        HTTPProvider = staticmethod(lambda *a, **kw: object())
        to_checksum_address = staticmethod(lambda a: a)
        def __init__(self, provider):
            self.middleware_onion = SimpleNamespace(inject=lambda middleware, layer: None)
            self.eth = SimpleNamespace(chain_id=chain,
                                       get_block=lambda block: {"number": 123, "timestamp": int(time.time())},
                                       contract=lambda **kw: SimpleNamespace(functions=funcs))
    monkeypatch.setitem(sys.modules, "web3", SimpleNamespace(Web3=Web3))
    if chain == 56 and nonce >= 0 and current_valid and (nonce == 0 or not old_valid):
        assert proxy._read_exchange_nonce({}, "exchange", "maker") == nonce
        assert all(c == {"block_identifier": 123} for c in calls)
    else:
        with pytest.raises(ValueError, match="exchange_nonce_read_failed"):
            proxy._read_exchange_nonce({}, "exchange", "maker")


def test_nonce_rpc_failure_does_not_fallback_or_leak_url(proxy, monkeypatch):
    class Web3:
        @staticmethod
        def HTTPProvider(*a, **kw):
            raise RuntimeError("https://private-rpc.invalid/secret")
    monkeypatch.setitem(sys.modules, "web3", SimpleNamespace(Web3=Web3))
    with pytest.raises(ValueError, match="^exchange_nonce_read_failed$"):
        proxy._read_exchange_nonce({}, "exchange", "maker")


@pytest.mark.parametrize("age", [31, 3600, -6])
def test_stale_or_future_rpc_block_refuses_nonce(proxy, monkeypatch, age):
    class Web3:
        HTTPProvider = staticmethod(lambda *a, **kw: object())
        def __init__(self, provider):
            self.middleware_onion = SimpleNamespace(inject=lambda middleware, layer: None)
            self.eth = SimpleNamespace(chain_id=56, get_block=lambda tag: {
                "number": 123, "timestamp": 10000 - age}, contract=fail)
    monkeypatch.setattr(proxy.time, "time", lambda: 10000)
    monkeypatch.setitem(sys.modules, "web3", SimpleNamespace(Web3=Web3))
    with pytest.raises(ValueError, match="exchange_nonce_read_failed"):
        proxy._read_exchange_nonce({}, "exchange", "maker")


def test_signing_uses_chain_nonce_and_legacy_mode_does_not_read_rpc(proxy, monkeypatch):
    owner = "0x" + "1" * 40
    calls = []
    account = SimpleNamespace(
        from_key=lambda key: SimpleNamespace(address=owner),
        sign_message=lambda *a, **kw: SimpleNamespace(signature=b"fake"))
    monkeypatch.setitem(sys.modules, "eth_account", SimpleNamespace(Account=account))
    monkeypatch.setitem(sys.modules, "eth_account.messages", SimpleNamespace(
        encode_defunct=lambda **kw: None, encode_typed_data=lambda **kw: None,
        _hash_eip191_message=lambda message: b"x" * 32))
    monkeypatch.setattr(proxy, "normalize_private_key", lambda key: "unused-test-value")
    monkeypatch.setattr(proxy, "_predict_account_digest", lambda *a: b"digest")
    def nonce(*args):
        calls.append(args)
        return 4
    monkeypatch.setattr(proxy, "_read_exchange_nonce", nonce)
    proxy.ORDER_LEDGER_FILE.write_text('{"orders":{}}')
    result = proxy._signed_order_payload(env(require_order_ledger=True, use_exchange_nonce=True), "account_01", body())
    assert result["signed_order"]["nonce"] == "4"
    assert len(calls) == 1
    result = proxy._signed_order_payload(env(require_order_ledger=True, use_exchange_nonce=True), "account_02", body())
    assert result["signed_order"]["nonce"] == "0"
    assert len(calls) == 1


def test_nonce_is_recorded_before_upstream_submit(proxy, monkeypatch):
    proxy.ORDER_LEDGER_FILE.write_text('{"orders":{}}')
    monkeypatch.setattr(proxy, "_signed_order_payload", lambda *a: {
        "order": {"nonce": "5", "expiration": "9999999999", "side": 0,
                  "makerAmount": str(8 * 10**17), "maker": "owner"},
        "signed_order": {}, "amounts": {"pricePerShare": "400000000000000000"},
        "order_hash": "0x" + "a" * 64, "signer_mode": "predict_account"})
    def upstream(*a, **kw):
        row = json.loads(proxy.ORDER_LEDGER_FILE.read_text())["orders"]["account_01:old:g2"]
        assert row["order_nonce"] == "5"
        assert row["error"] == "submission_pending"
        return 201, {"success": True, "data": {"orderId": "example-order"}}
    monkeypatch.setattr(proxy, "_authenticated_request", upstream)
    assert proxy.submit_order(env(require_order_ledger=True), "account_01", body())["ok"] is True
    row = json.loads(proxy.ORDER_LEDGER_FILE.read_text())["orders"]["account_01:old:g2"]
    assert row["order_nonce"] == "5"


@pytest.mark.parametrize("contents", [None, "{", "[]", '{"orders":[]}',
    '{"orders":{"account_01:old:g2":null}}',
    '{"orders":{"account_01:old:g2":{"quarantined":"true"}}}'])
@pytest.mark.parametrize("aliases", [("account_01", "account_02"), ("account_02", "account_01")])
@pytest.mark.parametrize("operation", ["submit", "status", "cancel"])
def test_shared_ledger_loss_cannot_be_bootstrapped_by_legacy_account(
    proxy, monkeypatch, contents, aliases, operation,
):
    settings = env(require_order_ledger=True, use_exchange_nonce=True)
    original = {"orders": {"account_01:old:g2": {"quarantined": True, "order_nonce": "0"}}}
    proxy.ORDER_LEDGER_FILE.write_text(json.dumps(original))
    assert proxy.submit_order(settings, "account_01", body())["error"] == "submission_quarantined"
    if contents is None:
        proxy.ORDER_LEDGER_FILE.unlink()
    else:
        proxy.ORDER_LEDGER_FILE.write_text(contents)
    monkeypatch.setattr(proxy, "_signed_order_payload", fail)
    monkeypatch.setattr(proxy, "_authenticated_request", fail)
    monkeypatch.setattr(proxy, "_cancel_gas_context", fail)
    monkeypatch.setattr(proxy, "_write_order_ledger", fail)
    for alias in aliases:
        with pytest.raises(ValueError, match="order_ledger"):
            if operation == "submit":
                proxy.submit_order(settings, alias, body())
            elif operation == "status":
                proxy.submission_status(settings, alias, "old:g2")
            else:
                proxy.cancel_orders_on_chain(settings, alias, {
                    "cancel": True, "confirm": "CANCEL_PREDICTFUN_ORDERS",
                    "hashes": ["0x" + "a" * 64]})
    assert (proxy.ORDER_LEDGER_FILE.read_text() if proxy.ORDER_LEDGER_FILE.exists() else None) == contents
    # Restoring the fixture proves neither account overwrote quarantine/nonce.
    proxy.ORDER_LEDGER_FILE.write_text(json.dumps(original))
    assert proxy.submission_status(settings, "account_01", "old:g2")["submission_state"] == "quarantined"
    assert json.loads(proxy.ORDER_LEDGER_FILE.read_text()) == original


@pytest.mark.parametrize("stage", ["before_post", "after_post", "status_refresh"])
def test_legacy_writer_rereads_shared_integrity_at_each_mutation(proxy, monkeypatch, stage):
    settings = env(require_order_ledger=True)
    original = {"orders": {"account_01:old:g2": {"quarantined": True, "order_nonce": "0"}}}
    if stage == "status_refresh":
        original["orders"]["account_02:other"] = {"ok": False, "error": "submission_pending",
            "order_hash": "0x" + "a" * 64}
    proxy.ORDER_LEDGER_FILE.write_text(json.dumps(original))
    posts = []
    def signed(*args):
        if stage == "before_post":
            proxy.ORDER_LEDGER_FILE.write_text("{")
        return {"order": {"nonce": "0", "expiration": "9999999999", "side": 0,
                "makerAmount": str(8 * 10**17), "maker": "owner"}, "signed_order": {},
                "amounts": {"pricePerShare": "400000000000000000"},
                "order_hash": "0x" + "a" * 64, "signer_mode": "predict_account"}
    def upstream(*args, **kwargs):
        posts.append(kwargs.get("method", "GET"))
        proxy.ORDER_LEDGER_FILE.write_text("{")
        return 200, {"success": True, "data": {"id": "test-order", "status": "OPEN"}}
    monkeypatch.setattr(proxy, "_signed_order_payload", signed)
    monkeypatch.setattr(proxy, "_authenticated_request", upstream)
    with pytest.raises(ValueError, match="order_ledger"):
        if stage == "status_refresh":
            proxy.submission_status(settings, "account_02", "other")
        else:
            proxy.submit_order(settings, "account_02", body("other"))
    assert proxy.ORDER_LEDGER_FILE.read_text() == "{"
    assert posts == ([] if stage == "before_post" else ["GET" if stage == "status_refresh" else "POST"])


def test_shared_policy_validates_all_accounts_without_short_circuit(proxy):
    settings = env(require_order_ledger=True)
    rows = json.loads(settings["PREDICTFUN_ACCOUNT_KEYS_JSON"])
    rows["account_02"]["require_order_ledger"] = "false"
    settings["PREDICTFUN_ACCOUNT_KEYS_JSON"] = json.dumps(rows)
    with pytest.raises(ValueError, match="must_be_boolean"):
        proxy._shared_ledger_requires_strict(settings)
    assert proxy._shared_ledger_requires_strict(env()) is False
    assert proxy._shared_ledger_requires_strict(env(require_order_ledger=True)) is True


def test_cancel_completion_cannot_rebuild_damaged_shared_ledger(proxy, monkeypatch):
    settings = env(require_order_ledger=True)
    proxy.ORDER_LEDGER_FILE.write_text(json.dumps({"orders": {
        "account_01:old:g2": {"quarantined": True, "order_nonce": "0"}}}))
    maker = "0x" + "2" * 40
    monkeypatch.setattr(proxy, "_cancel_gas_context", lambda *a: (
        {"predict_account": maker, "private_key": "synthetic-only"}, {"ok": True}))
    fake_builder = SimpleNamespace(cancel_orders=lambda *a: SimpleNamespace(
        success=True, receipt={"status": 1, "transactionHash": b"x" * 32}))
    monkeypatch.setitem(sys.modules, "predict_sdk", SimpleNamespace(
        CancelOrdersOptions=lambda **k: k, ChainId=SimpleNamespace(BNB_MAINNET=56),
        Order=lambda **k: k, OrderBuilder=SimpleNamespace(make=lambda *a: fake_builder),
        OrderBuilderOptions=lambda **k: k, Side=int, SignatureType=int))
    calls = []
    def upstream(*a, **kw):
        calls.append(kw.get("method", "GET"))
        if len(calls) == 1:
            return 200, {"success": True, "data": {"order": {"maker": maker}}}
        if len(calls) == 2:
            return 200, {"success": True}
        proxy.ORDER_LEDGER_FILE.write_text("{")
        return 200, {"success": True, "data": {"status": "CANCELLED"}}
    monkeypatch.setattr(proxy, "_authenticated_request", upstream)
    monkeypatch.setattr(proxy, "_write_order_ledger", fail)
    with pytest.raises(ValueError, match="order_ledger"):
        proxy.cancel_orders_on_chain(settings, "account_02", {
            "cancel": True, "confirm": "CANCEL_PREDICTFUN_ORDERS", "hashes": ["0x" + "a" * 64]})
    assert calls == ["GET", "POST", "GET"]
    assert proxy.ORDER_LEDGER_FILE.read_text() == "{"


def test_all_runtime_ledger_reads_have_explicit_integrity_policy(proxy):
    import ast
    tree = ast.parse(Path(proxy.__file__).read_text())
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == "_load_order_ledger"]
    assert len(calls) == 8
    for call in calls:
        assert len(call.keywords) == 1 and call.keywords[0].arg == "strict"
        value = call.keywords[0].value
        assert (isinstance(value, ast.Name) and value.id == "strict_ledger") or (
            isinstance(value, ast.Constant) and value.value is True)
