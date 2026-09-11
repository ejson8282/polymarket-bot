from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import hashlib
from types import SimpleNamespace

import pytest

from platforms.polymarket.maker import aggressive_acceptance as audit
from platforms.polymarket.maker.account_roster import parse_runtime_roster, market_universe_sha256


NOW = datetime(2026, 9, 11, 0, 0, tzinfo=timezone.utc)
MAKER = "0x" + "a" * 40
SIGNER = "0x" + "b" * 40
SPENDER = "0x" + "c" * 40
IDENTITY = {"maker_address": MAKER, "chain_id": 137, "signature_type": 2}


def make_legacy_release(parent, sha, monkeypatch):
    root = parent / sha
    root.mkdir()
    hashes = {}
    for name in audit._PINNED_LEGACY_FILES:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# synthetic reviewed source: " + name + "\n")
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(audit, "_PINNED_LEGACY_FILES", hashes)
    monkeypatch.setattr(audit, "_PINNED_LEGACY_SHA", sha)
    manifest = {"source_repository": "ejson8282/polymarket-bot", "commit": sha,
                "engine_sha256": hashes["platforms/polymarket/maker/engine.py"],
                "artifacts_sha256": {name: hashes[name] for name in audit._LEGACY_DECLARED_FILES}}
    (root / ".release-manifest.json").write_text(json.dumps(manifest))
    return root, manifest


@pytest.fixture
def legacy_release(tmp_path, monkeypatch):
    return make_legacy_release(tmp_path, audit._PINNED_LEGACY_SHA, monkeypatch)


@pytest.mark.parametrize("full_manifest", [False, True])
def test_pinned_legacy_checks_all_artifacts_without_rewriting(legacy_release, full_manifest):
    root, manifest = legacy_release
    if full_manifest:
        manifest["artifacts_sha256"] = dict(audit._PINNED_LEGACY_FILES)
        (root / ".release-manifest.json").write_text(json.dumps(manifest))
    before = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    result = audit.verify_runtime_release(root, root.name)
    assert result["method"] == "pinned_git_legacy_manifest"
    assert result["verified_artifacts"] == 19
    assert before == {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}


@pytest.mark.parametrize("name", sorted(audit._PINNED_LEGACY_FILES))
@pytest.mark.parametrize("failure", ["missing", "tamper", "symlink"])
def test_every_legacy_artifact_fails_closed(legacy_release, tmp_path, name, failure):
    root, manifest = legacy_release
    path = root / name
    original = path.read_bytes()
    path.unlink()
    if failure == "tamper":
        path.write_bytes(b"not reviewed")
        # A matching forged runtime digest is insufficient against the Git pin.
        if name in manifest["artifacts_sha256"]:
            manifest["artifacts_sha256"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
            (root / ".release-manifest.json").write_text(json.dumps(manifest))
    if failure == "symlink":
        outside = tmp_path / "outside"
        outside.write_bytes(original)
        path.symlink_to(outside)
    with pytest.raises(audit.AcceptanceError):
        audit.verify_runtime_release(root, root.name)


@pytest.mark.parametrize("change", ["repository", "commit", "engine", "extra", "missing", "hash", "wrong_type"])
def test_legacy_manifest_identity_and_declared_set_remain_strict(legacy_release, change):
    root, manifest = legacy_release
    if change in {"repository", "commit", "engine"}:
        key = {"repository": "source_repository", "commit": "commit", "engine": "engine_sha256"}[change]
        manifest[key] = "wrong"
    elif change == "extra":
        manifest["artifacts_sha256"]["unexpected.py"] = "f" * 64
    elif change == "missing":
        manifest["artifacts_sha256"].pop(next(iter(manifest["artifacts_sha256"])))
    elif change == "hash":
        manifest["artifacts_sha256"][next(iter(manifest["artifacts_sha256"]))] = "f" * 64
    else:
        manifest["artifacts_sha256"] = []
    (root / ".release-manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(audit.AcceptanceError):
        audit.verify_runtime_release(root, root.name)


def test_unknown_release_keeps_original_strict_verifier(tmp_path, monkeypatch):
    calls = []
    def strict(root, sha):
        calls.append((root, sha))
        raise RuntimeError("original verifier rejected")
    monkeypatch.setattr(audit, "_verify_release_manifest", strict)
    root, sha = tmp_path / ("b" * 40), "b" * 40
    with pytest.raises(RuntimeError, match="original verifier"):
        audit.verify_runtime_release(root, sha)
    assert calls == [(root, sha)]


def test_legacy_release_path_mismatch_rejected(legacy_release):
    root, _ = legacy_release
    renamed = root.with_name("wrong")
    root.rename(renamed)
    with pytest.raises(audit.AcceptanceError, match="path_mismatch"):
        audit.verify_runtime_release(renamed, root.name)


def test_legacy_catalog_covers_full_required_set():
    assert audit._PINNED_LEGACY_SHA == "6ff20650f93b3758e5a41599c7a86e22e131eddf"
    assert len(audit._PINNED_LEGACY_FILES) == 19
    assert len(audit._LEGACY_DECLARED_FILES) == 9
    assert audit._LEGACY_DECLARED_FILES < set(audit._PINNED_LEGACY_FILES)


def order(side="BUY", status="LIVE"):
    return {"id": "order1", "market": "condition1", "asset_id": "12", "maker_address": MAKER,
            "original_size": "20", "size_matched": "5", "side": side, "status": status, "price": "0.4"}


class Reads:
    def __init__(self):
        self.identity, self.source = IDENTITY, "synthetic"
        self.transport = self
        self.contracts = SimpleNamespace(exchange_v2=SPENDER)
        self.orders, self.trades, self.position_rows = [], [], []
        self.calls = []
        self.cash, self.chain_cash = "200000000", "200"
        self.chain_holdings = {}
        self.allowances = {SPENDER: "200000000"}
        self.fail = None

    def get(self, path, params):
        self.calls.append(path)
        if path == self.fail:
            raise RuntimeError("SECRET-DO-NOT-PRINT")
        if path == "/balance-allowance":
            return {"balance": self.cash, "allowances": self.allowances}
        if path == "/data/orders":
            return {"data": deepcopy(self.orders), "next_cursor": "LTE="}
        if path == "/data/trades":
            return {"data": deepcopy(self.trades), "next_cursor": "LTE="}
        if path == "/order-scoring":
            return {"scoring": True}
        pytest.fail("unexpected read")

    def positions(self):
        return {"rows": self.position_rows, "pagination_complete": True}

    def assets(self, tokens):
        return {"cash_units": self.chain_cash,
                "holdings": {t: self.chain_holdings.get(t, "0") for t in tokens}}


def collect(reads, clock=lambda: NOW):
    return audit.collect_acceptance(reads, IDENTITY, Decimal("200"), clock=clock)


def test_empty_paused_account_audit_is_not_scoring_or_live_permission():
    report = collect(Reads())
    assert report["account_audit_passed"] is True
    assert report["observation"]["scoring"] == {}
    assert report["market_admission"] == "not_evaluated"
    assert report["live_enabled"] is report["mutation_enabled"] is report["budget_admission_enabled"] is False
    assert "live_scoring_observation" in report["remaining_live_gates"]
    assert report["observation"]["samples"]["collateral"]["current"]["effective_capital_usdc"] is None


@pytest.mark.parametrize("status", ["LIVE", "PENDING", "DELAYED", "UNMATCHED", "ORDER_STATUS_LIVE"])
def test_buy_blocks_paused_audit_even_if_scoring_true(status):
    reads = Reads()
    reads.orders = [order(status=status)]
    report = collect(reads)
    assert report["account_audit_passed"] is False
    assert report["checks"]["zero_buy"] == "buy_present"


def test_sell_preserved_not_cancelled_and_position_reconciled():
    reads = Reads()
    reads.orders = [order(side="SELL")]
    reads.position_rows = [{"token_id": "12", "size": "15", "current_value": "6"}]
    reads.chain_holdings = {"12": "15"}
    report = collect(reads)
    assert report["account_audit_passed"]
    assert report["checks"]["discovered_positions_reconciliation"] == "pass"
    assert set(reads.calls) == {"/balance-allowance", "/data/orders", "/data/trades"}


@pytest.mark.parametrize("field,value,check", [
    ("chain_cash", "199", "cash_reconciliation"),
    ("cash", "190000000", "principal_available"),
    ("allowances", {}, "selected_exchange_allowance"),
    ("fail", "/data/orders", "orders"),
    ("fail", "/data/trades", "trades"),
])
def test_unknown_mismatch_or_insufficient_is_blocked(field, value, check):
    reads = Reads()
    setattr(reads, field, value)
    report = collect(reads)
    assert not report["account_audit_passed"]
    assert report["checks"][check] not in {"pass", "current"}
    assert "SECRET-DO-NOT-PRINT" not in json.dumps(report)


def test_position_mismatch_blocks():
    reads = Reads()
    reads.position_rows = [{"token_id": "12", "size": "15", "current_value": "6"}]
    assert collect(reads)["checks"]["discovered_positions_reconciliation"] == "mismatch"


def test_late_asset_read_expires_earlier_clob_sample():
    reads = Reads()
    times = [NOW]
    def assets(tokens):
        times[0] += timedelta(seconds=61)
        return {"cash_units": "200", "holdings": {}}
    reads.assets = assets
    report = collect(reads, lambda: times[0])
    assert not report["account_audit_passed"]
    assert report["checks"]["collateral"] == "stale"
    assert report["observation"]["samples"]["collateral"]["current"] is None


@pytest.fixture
def runtime():
    roster = {"runtime_scope": "aggressive", "accounts": [{"account_index": 1,
        "host_id": "aggressive-a", "funder": MAKER, "clash_port": 18081,
        "lp_account": {"account_id": "aggressive-a-1", "enabled": True,
                       "profile_type": "aggressive", "target_principal_usdc": "200"}}]}
    account = parse_runtime_roster(roster)[0]
    config = {"account": {"funder": MAKER, "signature_type": 2, "chain_id": 137,
                         "signer_server_url": "http://100.91.159.54:8421"},
              "lp_account": account.generation_entry()["lp_account"], "markets": []}
    contract = {"roster_sha256": "d" * 64, "market_sha256": market_universe_sha256(config),
                "signer_url": "http://100.91.159.54:8421"}
    config["runtime_account"] = {"account_index": 1, "host_id": "aggressive-a", "runtime_scope": "aggressive",
        "clash_port": 18081, "routing_roster_sha256": contract["roster_sha256"],
        "market_universe_sha256": contract["market_sha256"]}
    state = {"account_index": 1, "account_id": account.profile.account_id, "paused": True,
        "account_uid_key": hashlib.sha256(f"137:2:{MAKER}".encode()).hexdigest()[:16],
        "ts": NOW.isoformat(), "release_sha": "e" * 40, "release_required": True,
        "runtime": {"host_id": "aggressive-a", "scope": "aggressive",
                    "routing_roster_sha256": contract["roster_sha256"],
                    "market_universe_sha256": contract["market_sha256"]}}
    return account, config, state, contract, "e" * 40


def test_existing_profile_runtime_passes_and_supports_50(runtime):
    audit.check_runtime(*runtime, paused_marker=True, service_active=True, now=NOW)
    # Existing profile caps, not new fixed small-cap tiers, remain authoritative.
    assert runtime[0].profile.effective_available(Decimal("999")) == Decimal("200")
    account, config, state, contract, release = runtime
    config["lp_account"]["target_principal_usdc"] = "50"
    config["lp_account"]["pause_equity_usdc"] = "42.50"
    config["lp_account"]["daily_loss_limit_usdc"] = "2.50"
    profile = audit.parse_lp_account_profile(config, 1)
    assert profile.effective_available(Decimal("999")) == Decimal("50")


@pytest.mark.parametrize("field,value", [("paused", False), ("account_index", 2), ("account_index", True),
    ("release_sha", "0"*40), ("release_required", False), ("account_id", "stable-account"),
    ("ts", (NOW-timedelta(seconds=61)).isoformat()), ("ts", (NOW+timedelta(seconds=1)).isoformat())])
def test_bad_runtime_cannot_bootstrap_credentials(runtime, field, value):
    runtime[2][field] = value
    with pytest.raises(audit.AcceptanceError):
        audit.check_runtime(*runtime, paused_marker=True, service_active=True, now=NOW)


@pytest.mark.parametrize("marker,active", [(False, True), (True, False)])
def test_marker_and_active_service_both_required(runtime, marker, active):
    with pytest.raises(audit.AcceptanceError):
        audit.check_runtime(*runtime, paused_marker=marker, service_active=active, now=NOW)


def test_host_swap_and_config_funder_mismatch_block(runtime):
    runtime[2]["runtime"]["host_id"] = "aggressive-b"
    with pytest.raises(audit.AcceptanceError, match="runtime_contract_mismatch"):
        audit.check_runtime(*runtime, paused_marker=True, service_active=True, now=NOW)
    runtime[2]["runtime"]["host_id"] = "aggressive-a"
    runtime[1]["account"]["funder"] = SIGNER
    with pytest.raises(audit.AcceptanceError, match="config_funder_mismatch"):
        audit.check_runtime(*runtime, paused_marker=True, service_active=True, now=NOW)


def test_symlink_outside_runtime_rejected(tmp_path):
    root = tmp_path / "runtime"
    root.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}")
    (root / "state.json").symlink_to(outside)
    with pytest.raises(audit.AcceptanceError, match="runtime_path_escaped"):
        audit.read_json(root / "state.json", root)


class Response:
    status_code = 200
    def __init__(self, body): self.body = body
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def iter_content(self, size): yield json.dumps(self.body).encode()


def credentials():
    return {"funder": MAKER, "address": SIGNER, "chain_id": 137, "signature_type": 2,
            "mode": "existing_only", "api_key": "test-key", "api_secret": "dGVzdA==", "api_passphrase": "test-pass"}


def test_real_sdk_hmac_client_reads_only_get_with_finite_timeout(monkeypatch):
    client = audit.AccountReads(IDENTITY, credentials(), "http://127.0.0.1:18081")
    calls = []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response({"balance": "200000000", "allowances": {SPENDER: "200000000"}})
    monkeypatch.setattr(client.venue, "get", get)
    monkeypatch.setattr(client.venue, "post", lambda *a, **kw: pytest.fail("CLOB POST forbidden"))
    result = client.transport.get("/balance-allowance", {"asset_type": "COLLATERAL", "signature_type": 2})
    assert result["balance"] == "200000000"
    assert calls[0][0] == "https://clob.polymarket.com/balance-allowance"
    assert calls[0][1]["headers"]["POLY_ADDRESS"] == SIGNER
    assert calls[0][1]["timeout"] == (5, 15)
    assert calls[0][1]["allow_redirects"] is False
    with pytest.raises(ValueError, match="read_scope_violation"):
        client.transport.get("/order", {})
    client.close()


def test_client_identity_cannot_be_relabelled():
    creds = credentials()
    creds["funder"] = SIGNER
    with pytest.raises(audit.AcceptanceError, match="signer_identity_mismatch"):
        audit.AccountReads(IDENTITY, creds, "http://127.0.0.1:18081")


def test_public_positions_decimal_identity_and_bounded_pagination(monkeypatch):
    client = audit.AccountReads(IDENTITY, credentials(), "http://127.0.0.1:18081")
    body = [{"proxyWallet": MAKER, "asset": "12", "size": "15.000001", "currentValue": "6"}]
    monkeypatch.setattr(client.venue, "get", lambda *a, **kw: Response(body))
    assert client.positions()["rows"][0]["size"] == "15.000001"
    body[0]["proxyWallet"] = SIGNER
    with pytest.raises(audit.AcceptanceError, match="position_identity_mismatch"):
        client.positions()
    client.close()


def test_deadline_escapes_per_source_handlers_and_cli_redacts(monkeypatch, capsys):
    def fail(): raise audit.AcceptanceDeadline()
    with pytest.raises(audit.AcceptanceDeadline):
        audit.sample(fail, lambda: NOW)
    monkeypatch.setattr(audit, "run_host", lambda *a, **kw: fail())
    assert audit.main(["--profile", "aggressive-a", "--tooling-sha", "e"*40]) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["reason"] == "acceptance_deadline_exceeded"
    def secret_failure(*args): raise RuntimeError("SECRET-DO-NOT-PRINT")
    monkeypatch.setattr(audit, "run_host", secret_failure)
    assert audit.main(["--profile", "aggressive-b", "--tooling-sha", "e"*40]) == 2
    assert "SECRET-DO-NOT-PRINT" not in capsys.readouterr().out


def test_run_host_rechecks_paused_contract_after_reads(runtime, tmp_path, monkeypatch):
    account, config, state, contract, release = runtime
    config["proxy_pool"] = {"items": [{"url": "http://127.0.0.1:18081", "enabled": True}]}
    paths = SimpleNamespace(runtime_root=tmp_path, release_root=tmp_path,
                            config_dir=tmp_path, data_dir=tmp_path)
    (tmp_path / "config_1.json").write_text(json.dumps(config))
    state_path = tmp_path / "engine_state_1.json"
    state_path.write_text(json.dumps(state))
    (tmp_path / ".account_1.paused").touch()
    contract.update(local_accounts=[account], env={"SIGNER_TOKEN": "test-token"})
    monkeypatch.setattr(audit, "aggressive_paths_for_profile", lambda _: paths)
    monkeypatch.setattr(audit, "_current_release", lambda _: release)
    monkeypatch.setattr(audit, "_verify_release_manifest", lambda *a: {})
    monkeypatch.setattr(audit, "verify_tooling", lambda *a: {"commit": release, "manifest_sha256": "f"*64})
    monkeypatch.setattr(audit, "_runtime_contract", lambda *a: contract)
    monkeypatch.setattr(audit, "service_active", lambda: True)
    calls = []
    def signer(*args, **kwargs):
        calls.append("derive")
        return SimpleNamespace(derive_existing_creds=credentials, _session=SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(audit, "RemoteSignerClient", signer)
    reads = Reads()
    reads.close = lambda: None
    monkeypatch.setattr(audit, "AccountReads", lambda *args: reads)
    assert audit.run_host("aggressive-a", clock=lambda: NOW)["status"] == "pass"
    def changed(tokens):
        state["paused"] = False
        state_path.write_text(json.dumps(state))
        return {"cash_units": "200", "holdings": {}}
    reads.assets = changed
    report = audit.run_host("aggressive-a", clock=lambda: NOW)
    assert report["status"] == "blocked"
    assert report["accounts"][0]["reason"] == "aggressive_not_confirmed_paused"
    calls.clear()
    assert audit.run_host("aggressive-a", clock=lambda: NOW)["status"] == "blocked"
    assert calls == []


@pytest.fixture
def host_audit(runtime, tmp_path, monkeypatch):
    account, config, state, contract, release = runtime
    config["proxy_pool"] = {"items": [{"url": "http://127.0.0.1:18081", "enabled": True}]}
    paths = SimpleNamespace(runtime_root=tmp_path, release_root=tmp_path, config_dir=tmp_path, data_dir=tmp_path)
    contract.update(local_accounts=[account], env={"SIGNER_TOKEN": "test-token"})
    context = SimpleNamespace(config=config, state=state, contract=contract, reads=Reads(), calls=[],
                              now=NOW, creds=credentials(), root=tmp_path)
    def save():
        (tmp_path / "config_1.json").write_text(json.dumps(context.config))
        (tmp_path / "engine_state_1.json").write_text(json.dumps(context.state))
    context.save = save
    save()
    (tmp_path / ".account_1.paused").touch()
    monkeypatch.setattr(audit, "aggressive_paths_for_profile", lambda _: paths)
    monkeypatch.setattr(audit, "_current_release", lambda _: release)
    monkeypatch.setattr(audit, "_verify_release_manifest", lambda *a: {})
    monkeypatch.setattr(audit, "verify_tooling", lambda *a: {"commit": release, "manifest_sha256": "f"*64})
    monkeypatch.setattr(audit, "_runtime_contract", lambda *a: contract)
    monkeypatch.setattr(audit, "service_active", lambda: True)
    def derive():
        context.calls.append("derive")
        return context.creds
    monkeypatch.setattr(audit, "RemoteSignerClient", lambda *a, **kw:
        SimpleNamespace(derive_existing_creds=derive, _session=SimpleNamespace(close=lambda: None)))
    original_client = audit.AccountReads
    def client(identity, creds, proxy):
        verified = original_client(identity, creds, proxy)
        verified.close()
        context.reads.identity = identity
        context.reads.close = lambda: None
        return context.reads
    monkeypatch.setattr(audit, "AccountReads", client)
    context.run = lambda: audit.run_host("aggressive-a", tooling_sha=release, clock=lambda: context.now)
    return context


def test_host_acceptance_uses_pinned_legacy_integrity(host_audit, monkeypatch):
    sha = host_audit.state["release_sha"]
    make_legacy_release(host_audit.root, sha, monkeypatch)
    result = host_audit.run()
    assert result["status"] == "pass"
    assert result["runtime_integrity"]["verified_artifacts"] == 19
    assert result["runtime_integrity"]["method"] == "pinned_git_legacy_manifest"
    assert result["live_enabled"] is False
    assert result["mutation_enabled"] is False


def test_host_final_integrity_recheck_blocks_changed_legacy_source(host_audit, monkeypatch):
    sha = host_audit.state["release_sha"]
    root, _ = make_legacy_release(host_audit.root, sha, monkeypatch)
    def assets(tokens):
        (root / "platforms/polymarket/maker/account_profiles.py").write_text("tampered")
        return {"cash_units": "200", "holdings": {}}
    host_audit.reads.assets = assets
    with pytest.raises(audit.AcceptanceError, match="runtime_git_artifact_mismatch"):
        host_audit.run()


@pytest.mark.parametrize("mismatch", ["host", "chain", "uid", "missing_uid"])
def test_host_identity_mismatch_rejected_before_credentials(host_audit, mismatch):
    ctx = host_audit
    if mismatch == "host":
        ctx.config["rest_base_url"] = "https://example.invalid"
    elif mismatch == "chain":
        ctx.config["account"]["chain_id"] = 1
    elif mismatch == "uid":
        ctx.state["account_uid_key"] = hashlib.sha256(f"137:2:{SIGNER}".encode()).hexdigest()[:16]
    else:
        ctx.state.pop("account_uid_key")
    ctx.save()
    assert ctx.run()["status"] == "blocked"
    assert ctx.calls == []


def test_engine_default_eoa_signature_and_state_uid(host_audit):
    ctx = host_audit
    ctx.config["account"].pop("signature_type")
    ctx.state["account_uid_key"] = hashlib.sha256(f"137:0:{MAKER}".encode()).hexdigest()[:16]
    ctx.creds.update(signature_type=0, address=MAKER)
    ctx.save()
    assert ctx.run()["status"] == "pass"
    assert ctx.reads.identity["signature_type"] == 0
    ctx.creds["signature_type"] = 2
    assert ctx.run()["status"] == "blocked"


def test_final_runtime_recheck_reages_every_sample(host_audit, monkeypatch):
    ctx = host_audit
    def assets(tokens):
        ctx.now += timedelta(seconds=56)
        ctx.state["ts"] = ctx.now.isoformat()
        ctx.save()
        return {"cash_units": "200", "holdings": {}}
    ctx.reads.assets = assets
    checks = []
    def active():
        checks.append(True)
        if len(checks) == 2:
            ctx.now += timedelta(seconds=5)
        return True
    monkeypatch.setattr(audit, "service_active", active)
    report = ctx.run()
    assert report["status"] == "blocked"
    assert report["generated_at"] == (NOW + timedelta(seconds=61)).isoformat()
    row = report["accounts"][0]
    assert row["checks"]["collateral"] == "stale"
    assert row["checks"]["cash_reconciliation"] == "unknown"
    assert row["observation"]["samples"]["collateral"]["current"] is None


def test_final_tooling_recheck_latency_also_expires_samples(host_audit, monkeypatch):
    ctx = host_audit
    checks = []
    def tooling(sha):
        checks.append(1)
        if len(checks) > 1:
            ctx.now += timedelta(seconds=61)
        return {"commit": sha, "manifest_sha256": "f"*64}
    monkeypatch.setattr(audit, "verify_tooling", tooling)
    report = ctx.run()
    assert report["status"] == "blocked"
    assert report["accounts"][0]["positions"]["current"] is None


def test_identity_change_during_reads_cannot_relabel_old_evidence(host_audit):
    ctx = host_audit
    def assets(tokens):
        ctx.config["account"]["signature_type"] = 0
        ctx.state["account_uid_key"] = hashlib.sha256(f"137:0:{MAKER}".encode()).hexdigest()[:16]
        ctx.save()
        return {"cash_units": "200", "holdings": {}}
    ctx.reads.assets = assets
    result = ctx.run()
    assert result["status"] == "blocked"
    assert result["accounts"][0]["reason"] == "config_changed_during_audit"


@pytest.fixture
def tooling_bundle(tmp_path, monkeypatch):
    commit = "a"*40
    root = tmp_path / commit
    hashes = {}
    for name in audit.TOOLING_FILES:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# reviewed source\n")
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {"repository": "ejson8282/polymarket-bot", "commit": commit, "files": hashes}
    (root / ".tooling-manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(audit, "__file__", str(root / "platforms/polymarket/maker/aggressive_acceptance.py"))
    return root, commit, manifest


def test_tooling_sha_is_distinct_and_required(tooling_bundle):
    root, commit, manifest = tooling_bundle
    assert audit.verify_tooling(commit)["commit"] == commit
    for wrong in (None, commit[:8], "b"*40):
        with pytest.raises(audit.AcceptanceError):
            audit.verify_tooling(wrong)


@pytest.mark.parametrize("name", sorted(audit.TOOLING_FILES))
def test_every_tooling_module_is_covered_by_manifest(tooling_bundle, name):
    root, commit, manifest = tooling_bundle
    (root / name).write_text("# changed source\n")
    with pytest.raises(audit.AcceptanceError, match="tooling_source_mismatch"):
        audit.verify_tooling(commit)


def test_tooling_manifest_cannot_omit_source(tooling_bundle):
    root, commit, manifest = tooling_bundle
    manifest["files"].pop("platforms/polymarket/maker/remote_signer.py")
    (root / ".tooling-manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(audit.AcceptanceError, match="tooling_manifest_mismatch"):
        audit.verify_tooling(commit)


@pytest.mark.parametrize("change", [None, "proxy", "identity", "unpause", "stale"])
def test_final_sweep_rechecks_each_account_not_only_the_last(host_audit, monkeypatch, change):
    ctx = host_audit
    second = parse_runtime_roster({"runtime_scope": "aggressive", "accounts": [{
        "account_index": 2, "host_id": "aggressive-a", "funder": SIGNER, "clash_port": 18082,
        "lp_account": {"account_id": "aggressive-a-2", "enabled": True,
                       "profile_type": "aggressive", "target_principal_usdc": "200"}}]})[0]
    ctx.contract["local_accounts"].append(second)
    config = deepcopy(ctx.config)
    config["account"]["funder"] = SIGNER
    config["lp_account"] = second.generation_entry()["lp_account"]
    config["runtime_account"].update(account_index=2, clash_port=18082)
    config["proxy_pool"]["items"][0]["url"] = "http://127.0.0.1:18082"
    state = deepcopy(ctx.state)
    state.update(account_index=2, account_id=second.profile.account_id,
                 account_uid_key=hashlib.sha256(f"137:2:{SIGNER}".encode()).hexdigest()[:16])
    (ctx.root / "config_2.json").write_text(json.dumps(config))
    (ctx.root / "engine_state_2.json").write_text(json.dumps(state))
    (ctx.root / ".account_2.paused").touch()
    if change == "stale":
        ctx.state["ts"] = (NOW - timedelta(seconds=59)).isoformat()
        ctx.save()
    monkeypatch.setattr(audit, "RemoteSignerClient", lambda *a, funder, **kw:
        SimpleNamespace(derive_existing_creds=lambda: {**credentials(), "funder": funder},
                        _session=SimpleNamespace(close=lambda: None)))
    def client(identity, creds, proxy):
        reads = Reads()
        reads.identity, reads.close = identity, lambda: None
        if identity["maker_address"] == SIGNER:
            def assets(tokens):
                if change == "proxy":
                    ctx.config["proxy_pool"]["items"][0]["url"] = "http://127.0.0.1:18999"
                elif change == "identity":
                    ctx.config["account"]["signature_type"] = 0
                    ctx.state["account_uid_key"] = hashlib.sha256(f"137:0:{MAKER}".encode()).hexdigest()[:16]
                elif change == "unpause":
                    ctx.state["paused"] = False
                    (ctx.root / ".account_1.paused").unlink()
                elif change == "stale":
                    ctx.now += timedelta(seconds=2)
                ctx.save()
                return {"cash_units": "200", "holdings": {}}
            reads.assets = assets
        return reads
    monkeypatch.setattr(audit, "AccountReads", client)
    report = ctx.run()
    assert len(report["accounts"]) == 2
    assert report["status"] == ("pass" if change is None else "blocked")
    assert report["accounts"][0]["runtime_paused_verified"] is (change is None)
    assert report["accounts"][0]["account_audit_passed"] is (change is None)
    assert report["accounts"][1]["runtime_paused_verified"] is True


def test_chain_assets_exact_units_and_read_only_rpc(monkeypatch):
    client = audit.AccountReads(IDENTITY, credentials(), "http://127.0.0.1:18081")
    calls = []
    block_hash = "0x" + "d" * 64
    header = {"hash": block_hash, "timestamp": hex(int(NOW.timestamp()))}
    def rpc(method, params):
        calls.append((method, params))
        if method == "eth_chainId": return "0x89"
        if method == "eth_blockNumber": return "0x100"
        if method == "eth_getBlockByNumber": return header
        assert method == "eth_call"
        assert params[1] == "0xfd"
        data = params[0]["data"]
        value = 6 if data == "0x313ce567" else (201688043 if data.startswith("0x70a08231") else 15000001)
        return "0x" + format(value, "064x")
    monkeypatch.setattr(client, "rpc", rpc)
    monkeypatch.setattr(audit.time, "time", lambda: NOW.timestamp())
    result = client.assets(["12"])
    assert result["cash_units"] == "201.688043"
    assert result["holdings"] == {"12": "15.000001"}
    assert result["block_hash"] == block_hash
    assert calls[-1][0] == "eth_getBlockByNumber"
    assert Decimal(audit.base_units(2**256-1)) == Decimal(str(2**256-1)[:-6] + "." + str(2**256-1)[-6:])
    client.close()


@pytest.mark.parametrize("failure", ["chain", "decimals", "hash", "stale", "result"])
def test_chain_inconsistency_fails_closed(monkeypatch, failure):
    client = audit.AccountReads(IDENTITY, credentials(), "http://127.0.0.1:18081")
    headers = []
    def rpc(method, params):
        if method == "eth_chainId": return "0x1" if failure == "chain" else "0x89"
        if method == "eth_blockNumber": return "0x100"
        if method == "eth_getBlockByNumber":
            headers.append(1)
            return {"timestamp": hex(int(NOW.timestamp()) - (121 if failure == "stale" else 0)),
                    "hash": "0x" + ("e" if failure == "hash" and len(headers) > 1 else "d") * 64}
        value = 18 if failure == "decimals" else 6
        return "0x" if failure == "result" else "0x" + format(value, "064x")
    monkeypatch.setattr(client, "rpc", rpc)
    monkeypatch.setattr(audit.time, "time", lambda: NOW.timestamp())
    with pytest.raises(audit.AcceptanceError):
        client.assets([])
    client.close()


def test_rpc_method_allowlist_and_http_error(monkeypatch):
    client = audit.AccountReads(IDENTITY, credentials(), "http://127.0.0.1:18081")
    with pytest.raises(audit.AcceptanceError, match="rpc_read_scope"):
        client.rpc("eth_sendRawTransaction", [])
    calls = []
    def post(url, **kwargs):
        calls.append((url, kwargs))
        return Response({"result": "0x89"})
    monkeypatch.setattr(client.chain, "post", post)
    assert client.rpc("eth_chainId", []) == "0x89"
    assert calls[0][1]["timeout"] == (5, 15)
    assert calls[0][1]["allow_redirects"] is False
    response = Response({})
    response.status_code = 302
    with pytest.raises(audit.AcceptanceError, match="http_read_unavailable"):
        audit.bounded_json(response)
    client.close()


def test_duplicate_positions_and_response_size_rejected(monkeypatch):
    client = audit.AccountReads(IDENTITY, credentials(), "http://127.0.0.1:18081")
    row = {"proxyWallet": MAKER, "asset": "12", "size": "15", "currentValue": "6"}
    monkeypatch.setattr(client.venue, "get", lambda *a, **kw: Response([row, row]))
    with pytest.raises(audit.AcceptanceError, match="position_duplicate"):
        client.positions()
    response = Response({})
    response.iter_content = lambda _: iter([b"x" * 5_000_001])
    with pytest.raises(audit.AcceptanceError, match="response_too_large"):
        audit.bounded_json(response)
    client.close()
