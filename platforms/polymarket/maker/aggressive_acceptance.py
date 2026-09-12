"""Read-only acceptance for the EXISTING isolated aggressive runtime.

No engine imports, account generation, credential creation, commands or writes.
A passing paused-account audit is not a market/scoring or live-trading approval.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import logging
from pathlib import Path
import re
import signal
import subprocess
import time

import requests

from .account_profiles import parse_lp_account_profile
from .account_roster import market_universe_sha256
from .deploy_aggressive_runtime import (
    SERVICE_NAME, aggressive_paths_for_profile, _current_release,
    _runtime_contract, _verify_release_manifest,
    _load_sanitized_base_config, apply_market_universe, load_json_object,
)
from .remote_signer import AddressStub, BuilderStub, RemoteSignerClient
from .small_cap_observation import ClobReadTransport, collect_account_observation, observation_at


TOOLING_FILES = {"platforms/polymarket/maker/" + name + ".py" for name in (
    "aggressive_acceptance", "account_profiles", "account_roster", "deploy_aggressive_runtime",
    "deploy_release", "market_universe", "remote_signer", "reward_ledger", "small_cap_observation")}


# Git-derived hashes, not values copied from a runtime manifest.
_PINNED_LEGACY_SHA = "6ff20650f93b3758e5a41599c7a86e22e131eddf"
_PINNED_LEGACY_FILES = {
    "platforms/polymarket/maker/account_profiles.py": "ad527beef524adcec92912df542117d3471c1bff7b750a5e4a79a25e9825dc18",
    "platforms/polymarket/maker/account_roster.py": "c539485f0dbcd610e709e0be205dc59631544b1e60fc57a72c5d298ab7e982b1",
    "platforms/polymarket/maker/aggressive_proxy.py": "721284f8bb22b27688e20fcbeeb0d50fe8da198d8dac3712e7ee137c053445b5",
    "platforms/polymarket/maker/aggressive_recovery.py": "e5dba2bdd24ea32768bd3ca7579907b2a66ccd828feb5a6b9cf462a5b7cea8a6",
    "platforms/polymarket/maker/engine.py": "f5266f89cf2403e25ac830a70635984a4976101696b8e7d0a580c44f2bd194c1",
    "platforms/polymarket/maker/exchange_maintenance.py": "6a7de493540e64e0c392fa2d399274c0869388f62be3605c77c92a4205f3c0c9",
    "platforms/polymarket/maker/multi_runner.py": "476205cff372d95635e0e003d502c4999dfa7fdf667b5b3089a868e476227ac8",
    "platforms/polymarket/maker/order_scoring_observer.py": "848d64528e520dc539e19539fff0f028cc6d0b799d9847712223e54f0deaa106",
    "platforms/polymarket/maker/quote_feasibility.py": "3d2a774b6771405392eed59ae72360137179ad0517e0cc5e3934fdffddd999ab",
    "platforms/polymarket/maker/release_guard.py": "df3e3a966427a6ee2da798548db7f821c79d1f2aa6e7b24843ab5bf4b0b47acd",
    "platforms/polymarket/maker/reward_fast_lane.py": "a1e342040ccb2044e8f01d2508b7ffa86b2fcd463e3b1f76779b6f06d3931cb0",
    "platforms/polymarket/maker/reward_observer.py": "e6b85306f6ebd89aa89022150bfe76101a7df53e74b8e4b6c350a0f93ce23a07",
    "platforms/polymarket/maker/reward_shadow_allocator.py": "dec4b67d9c0b059af89f44097132f1be82cd69979980eae159adf8549ba24193",
    "platforms/polymarket/maker/sibling_registry.py": "26c7d9c6f5a36730e60506f677db24e4b6ba3c72ef51aeb831a0e77f3ef29fec",
    "platforms/polymarket/maker/stable_lifecycle_commands.py": "87687e16e8b428bb5dc2cdb730c5a0f344d70e583909c4c4615cd1dfbe7db017",
    "platforms/polymarket/maker/stable_market_lifecycle.py": "c0905a8a34bd452cc12521ab33710a198b64e36ec630a799d4c6c3a81ef3c1ef",
    "platforms/polymarket/maker/stable_rotation_commands.py": "46734dcca32e82f7d08f58d9a886059d65adac45c8aa2e024ce0f4c80be1441a",
    "platforms/polymarket/maker/stable_rotation_planner.py": "9f4521691c78e9838961c2c9490afc8d96fc9294943ed4cad5e323d9e8d6dbbb",
    "platforms/polymarket/maker/stage_aggressive_market.py": "a8b7bef84ae4f1db8480ffb134b2ffe0b9e684f811ee35a7b831152b99bb1f42"
}
_LEGACY_DECLARED_FILES = {"platforms/polymarket/maker/" + name + ".py" for name in (
    "engine", "multi_runner", "aggressive_proxy", "reward_observer",
    "stable_rotation_planner", "stable_rotation_commands", "order_scoring_observer",
    "aggressive_recovery", "stage_aggressive_market")}


class AcceptanceError(ValueError):
    """Public error code, without raw payloads or credentials."""


class AcceptanceDeadline(BaseException):
    """Escape per-source error handlers when the CLI wall-clock budget expires."""


def require(ok, code):
    if not ok:
        raise AcceptanceError(code)


def utcnow():
    return datetime.now(timezone.utc)


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "timestamp_timezone_missing")
    return parsed


def amount(value):
    require(type(value) in (str, int, Decimal), "amount_type_invalid")
    result = Decimal(value)
    require(result.is_finite() and result >= 0, "amount_invalid")
    return result


def address(value):
    require(isinstance(value, str) and re.fullmatch(r"0x[0-9a-fA-F]{40}", value), "address_invalid")
    return value.lower()


def base_units(value):
    with localcontext() as context:
        context.prec = 100
        return format(Decimal(value).scaleb(-6), "f")


def read_json(path, root):
    require(path.resolve().is_relative_to(root.resolve()), "runtime_path_escaped")
    require(path.stat().st_size <= 5_000_000, "runtime_file_too_large")
    body = json.loads(path.read_text())
    require(isinstance(body, dict), "runtime_object_required")
    return body


def verify_tooling(expected_sha):
    require(isinstance(expected_sha, str) and re.fullmatch(r"[0-9a-f]{40}", expected_sha),
            "tooling_full_sha_required")
    root = Path(__file__).resolve().parents[3]
    require(root.name == expected_sha, "tooling_release_path_mismatch")
    manifest_path = root / ".tooling-manifest.json"
    manifest = read_json(manifest_path, root)
    require(manifest.get("repository") == "ejson8282/polymarket-bot" and
            manifest.get("commit") == expected_sha and isinstance(manifest.get("files"), dict) and
            set(manifest["files"]) == TOOLING_FILES, "tooling_manifest_mismatch")
    for name, digest in manifest["files"].items():
        path = root / name
        require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) and
                path.resolve().is_relative_to(root) and path.is_file() and not path.is_symlink() and
                path.stat().st_size <= 500_000 and hashlib.sha256(path.read_bytes()).hexdigest() == digest,
                "tooling_source_mismatch")
    return {"commit": expected_sha, "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}


def verify_runtime_release(release_dir, target_sha):
    if target_sha != _PINNED_LEGACY_SHA:
        _verify_release_manifest(release_dir, target_sha)
        return {"method": "release_manifest", "commit": target_sha}
    require(release_dir.name == target_sha and not release_dir.is_symlink(),
            "runtime_release_path_mismatch")
    manifest_path = release_dir / ".release-manifest.json"
    manifest = read_json(manifest_path, release_dir)
    require(manifest.get("source_repository") == "ejson8282/polymarket-bot"
            and manifest.get("commit") == target_sha, "runtime_manifest_identity_mismatch")
    declared = manifest.get("artifacts_sha256")
    require(isinstance(declared, dict) and set(declared) in (
        _LEGACY_DECLARED_FILES, set(_PINNED_LEGACY_FILES)), "runtime_manifest_artifact_set_mismatch")
    require(manifest.get("engine_sha256") ==
            _PINNED_LEGACY_FILES["platforms/polymarket/maker/engine.py"],
            "runtime_manifest_engine_mismatch")
    for name, expected in _PINNED_LEGACY_FILES.items():
        path = release_dir / name
        require(path.resolve().is_relative_to(release_dir.resolve()) and not path.is_symlink()
                and path.is_file() and path.stat().st_size <= 5_000_000
                and hashlib.sha256(path.read_bytes()).hexdigest() == expected,
                "runtime_git_artifact_mismatch")
        require(name not in declared or declared[name] == expected,
                "runtime_manifest_artifact_hash_mismatch")
    return {"method": "pinned_git_legacy_manifest", "commit": target_sha,
            "verified_artifacts": len(_PINNED_LEGACY_FILES),
            "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}


def configured_identity(config):
    host = config.get("rest_base_url", "https://clob.polymarket.com")
    require(isinstance(host, str) and host.rstrip("/") == "https://clob.polymarket.com",
            "config_clob_host_mismatch")
    account = config.get("account")
    require(isinstance(account, dict), "config_account_invalid")
    values = {}
    # These defaults and integer string semantics match the existing engine.
    for name, default, allowed in (("chain_id", 137, {137}), ("signature_type", 0, {0, 1, 2})):
        raw = account.get(name, default)
        require(type(raw) in (int, str) and re.fullmatch(r"[0-9]+", str(raw)), "config_" + name + "_invalid")
        values[name] = int(raw)
        require(values[name] in allowed, "config_" + name + "_mismatch")
    return {**values, "maker_address": address(account.get("funder"))}


def identity_key(identity):
    raw = f"{identity['chain_id']}:{identity['signature_type']}:{identity['maker_address']}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def bounded_json(response):
    with response:
        require(response.status_code == 200, "http_read_unavailable")
        chunks, size = [], 0
        for chunk in response.iter_content(65536):
            size += len(chunk)
            require(size <= 5_000_000, "response_too_large")
            chunks.append(chunk)
        return json.loads(b"".join(chunks), parse_float=Decimal)


class AccountReads:
    """Dedicated finite-timeout HTTP clients; CLOB requests are GET only."""

    def __init__(self, identity, credentials, proxy):
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds
        from py_clob_client_v2.config import get_contract_config

        require(credentials["funder"] == identity["maker_address"] and
                credentials["chain_id"] == identity["chain_id"] and
                credentials["signature_type"] == identity["signature_type"], "signer_identity_mismatch")
        self.identity = identity
        self.contracts = get_contract_config(137)
        self.venue = requests.Session()
        self.venue.trust_env = False
        self.venue.proxies = {"https": proxy}
        self.chain = requests.Session()
        self.chain.trust_env = False
        client = ClobClient("https://clob.polymarket.com", 137, creds=ApiCreds(
            api_key=credentials["api_key"], api_secret=credentials["api_secret"],
            api_passphrase=credentials["api_passphrase"]), use_server_time=False)
        client.signer = AddressStub(credentials["address"], 137)
        client.builder = BuilderStub(identity["signature_type"], identity["maker_address"])
        client.mode = client._get_client_mode()
        client._get = self.clob_get
        self.transport = ClobReadTransport(client, identity)

    def close(self):
        self.venue.close()
        self.chain.close()

    def clob_get(self, url, *, headers, params):
        require(url.startswith("https://clob.polymarket.com/"), "clob_destination_invalid")
        return bounded_json(self.venue.get(url, headers=headers, params=params,
                            timeout=(5, 15), allow_redirects=False, stream=True))

    def positions(self):
        maker, rows = self.identity["maker_address"], {}
        for offset in range(0, 2000, 500):
            body = bounded_json(self.venue.get("https://data-api.polymarket.com/positions",
                params={"user": maker, "sizeThreshold": 0, "limit": 500, "offset": offset},
                timeout=(5, 15), allow_redirects=False, stream=True))
            require(isinstance(body, list) and len(body) <= 500, "positions_invalid")
            for raw in body:
                require(address(raw.get("proxyWallet")) == maker, "position_identity_mismatch")
                token = raw.get("asset")
                require(isinstance(token, str) and token.isdigit() and len(token) <= 78 and
                        int(token) < 2**256, "position_token_invalid")
                require(token not in rows, "position_duplicate_or_unstable_pages")
                rows[token] = {"token_id": token, "size": str(amount(raw.get("size"))),
                               "current_value": str(amount(raw.get("currentValue")))}
            if len(body) < 500:
                return {"rows": list(rows.values()), "pagination_complete": True,
                        "coverage": "public_data_api_not_exhaustive_chain_inventory"}
        raise AcceptanceError("position_page_limit")

    def rpc(self, method, params):
        require(method in {"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_call"}, "rpc_read_scope")
        body = bounded_json(self.chain.post("https://polygon-bor-rpc.publicnode.com",
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=(5, 15), allow_redirects=False, stream=True))
        require(isinstance(body, dict) and "result" in body and "error" not in body, "rpc_unavailable")
        return body["result"]

    def assets(self, tokens):
        require(len(tokens) <= 40 and all(isinstance(t, str) and t.isdigit() and
                len(t) <= 78 and int(t) < 2**256 for t in tokens), "chain_token_limit_or_invalid")
        require(int(self.rpc("eth_chainId", []), 16) == 137, "chain_mismatch")
        block = hex(int(self.rpc("eth_blockNumber", []), 16) - 3)
        header = self.rpc("eth_getBlockByNumber", [block, False])
        require(isinstance(header, dict) and 0 <= time.time() - int(header["timestamp"], 16) <= 120,
                "chain_block_stale")
        require(isinstance(header.get("hash"), str) and re.fullmatch(r"0x[0-9a-fA-F]{64}", header["hash"]),
                "chain_block_hash_invalid")
        def call(contract, data):
            result = self.rpc("eth_call", [{"to": address(contract), "data": "0x" + data}, block])
            require(isinstance(result, str) and re.fullmatch(r"0x[0-9a-fA-F]{64}", result), "chain_result_invalid")
            return int(result, 16)
        maker = self.identity["maker_address"][2:].rjust(64, "0")
        collateral = address(self.contracts.collateral)
        require(call(collateral, "313ce567") == 6, "collateral_decimals_mismatch")
        cash = base_units(call(collateral, "70a08231" + maker))
        holdings = {t: base_units(call(self.contracts.conditional_tokens,
                    "00fdd58e" + maker + hex(int(t))[2:].rjust(64, "0"))) for t in tokens}
        require(self.rpc("eth_getBlockByNumber", [block, False])["hash"] == header["hash"], "chain_block_changed")
        return {"cash_units": str(cash), "collateral_address": collateral, "decimals": 6,
                "block": int(block, 16), "block_hash": header["hash"], "holdings": holdings,
                "coverage": "discovered_tokens_only"}


def approved_market_config(paths, expected_sha):
    config = apply_market_universe(_load_sanitized_base_config(paths.base_config),
                                   load_json_object(paths.market_universe))
    require(market_universe_sha256(config) == expected_sha, "approved_market_reference_mismatch")
    return config


def _paused_market_integrity(config, expected_sha, approved):
    actual_sha = market_universe_sha256(config)
    proof = {"method": "exact", "approved_sha256": expected_sha,
             "actual_sha256": actual_sha, "disabled_markets": 0}
    if actual_sha == expected_sha:
        return proof
    require(isinstance(approved, dict) and market_universe_sha256(approved) == expected_sha,
            "approved_market_reference_required")
    # Reconstruct only explicit true -> false restrictions; never normalize other drift.
    reconstructed, disabled = deepcopy(config), 0
    for section in ("markets", "night_markets"):
        wanted, actual = approved.get(section, []), reconstructed.get(section, [])
        require(isinstance(wanted, list) and isinstance(actual, list), "market_rows_invalid")
        indices = []
        for rows in (wanted, actual):
            index = {}
            for row in rows:
                require(isinstance(row, dict) and isinstance(row.get("token_id"), str)
                        and bool(row["token_id"]) and row["token_id"] not in index,
                        "market_identity_invalid_or_duplicate")
                index[row["token_id"]] = row
            indices.append(index)
        wanted_by_id, actual_by_id = indices
        require(wanted_by_id.keys() == actual_by_id.keys(), "market_set_mismatch")
        for token, row in actual_by_id.items():
            expected = wanted_by_id[token]
            require(type(expected.get("enabled")) is bool and type(row.get("enabled")) is bool,
                    "explicit_market_enabled_required")
            if expected["enabled"] is True and row["enabled"] is False:
                row["enabled"] = True
                disabled += 1
    require(disabled > 0 and market_universe_sha256(reconstructed) == expected_sha,
            "config_market_drift")
    return {**proof, "method": "paused_restrictive_disable", "disabled_markets": disabled}


def check_runtime(account, config, state, contract, release, *, paused_marker, service_active, now,
                  approved_markets=None):
    require(service_active, "aggressive_service_not_active")
    require(paused_marker and state.get("paused") is True, "aggressive_not_confirmed_paused")
    require(type(state.get("account_index")) is int and state["account_index"] == account.account_index,
            "runtime_account_mismatch")
    require(state.get("account_id") == account.profile.account_id, "runtime_account_id_mismatch")
    require(0 <= (now - timestamp(state.get("ts"))).total_seconds() <= 60, "runtime_state_stale_or_future")
    require(state.get("release_sha") == release and state.get("release_required") is True, "runtime_release_mismatch")
    runtime = state.get("runtime") or {}
    require(runtime.get("scope") == "aggressive" and runtime.get("host_id") == account.host_id and
            runtime.get("routing_roster_sha256") == contract["roster_sha256"] and
            runtime.get("market_universe_sha256") == contract["market_sha256"], "runtime_contract_mismatch")
    require(account.enabled and account.profile.managed and account.profile.profile_type == "aggressive",
            "aggressive_profile_required")
    require(parse_lp_account_profile(config, account.account_index) == account.profile, "profile_config_mismatch")
    require(address((config.get("account") or {}).get("funder")) == account.funder.lower(), "config_funder_mismatch")
    identity = configured_identity(config)
    require(state.get("account_uid_key") == identity_key(identity), "runtime_account_uid_mismatch")
    metadata = config.get("runtime_account") or {}
    require(metadata.get("account_index") == account.account_index and metadata.get("host_id") == account.host_id
            and metadata.get("runtime_scope") == "aggressive" and metadata.get("clash_port") == account.clash_port
            and metadata.get("routing_roster_sha256") == contract["roster_sha256"]
            and metadata.get("market_universe_sha256") == contract["market_sha256"], "config_contract_mismatch")
    require((config.get("account") or {}).get("signer_server_url", "").rstrip("/") == contract["signer_url"],
            "config_signer_mismatch")
    return _paused_market_integrity(config, contract["market_sha256"], approved_markets)


def sample(fn, clock):
    started = clock()
    try:
        value = fn()
        reason = None
    except AcceptanceError as exc:
        value, reason = None, str(exc)
    except Exception:
        value, reason = None, "read_unavailable"
    finished = clock()
    fresh = 0 <= (finished - started).total_seconds() <= 60
    return {"status": "unknown" if reason else ("current" if fresh else "stale"),
            "reason": reason or (None if fresh else "sample_expired"),
            "observed_from": started.isoformat(), "observed_to": finished.isoformat(),
            "current": value if fresh and not reason else None}


def collect_acceptance(reads, identity, principal, *, clock=utcnow):
    now = clock()
    observation = collect_account_observation(reads.transport, identity,
        collateral_spender=reads.contracts.exchange_v2, trade_after=int((now-timedelta(days=1)).timestamp()),
        trade_before=int(now.timestamp()), clock=clock, max_pages=10, max_rows=1000,
        max_scoring=20, max_age_sec=60)
    positions = sample(reads.positions, clock)
    tokens = {p["token_id"] for p in (positions["current"] or {}).get("rows", [])}
    for name in ("orders", "trades"):
        tokens.update(o["token_id"] for o in (observation["samples"][name]["current"] or {}).get("rows", []))
    assets = sample(lambda: reads.assets(sorted(tokens)), clock)
    return assess_acceptance(observation, positions, assets, principal, now=clock())


def assess_acceptance(observation, positions, assets, principal, *, now):
    observation = observation_at(observation, now=now)
    positions, assets = deepcopy(positions), deepcopy(assets)
    for item in (positions, assets):
        if item["status"] == "current" and not (
                0 <= (now-timestamp(item["observed_from"])).total_seconds() <= 60 and
                timestamp(item["observed_from"]) <= timestamp(item["observed_to"]) <= now):
            item.update(status="stale", reason="sample_expired", current=None)
    checks = {name: item["status"] for name, item in observation["samples"].items()}
    checks.update(positions=positions["status"], chain_assets=assets["status"])
    collateral = observation["samples"]["collateral"]["current"]
    orders = observation["samples"]["orders"]["current"]
    if collateral and assets["current"]:
        checks["cash_reconciliation"] = "pass" if amount(collateral["balance_usdc"]) == amount(assets["current"]["cash_units"]) else "mismatch"
        checks["principal_available"] = "pass" if amount(collateral["balance_usdc"]) >= principal else "insufficient"
        allowance = collateral["allowance_usdc"]
        checks["selected_exchange_allowance"] = "pass" if allowance is not None and amount(allowance) >= principal else "unknown_or_insufficient"
    else:
        checks.update(cash_reconciliation="unknown", principal_available="unknown", selected_exchange_allowance="unknown")
    checks["zero_buy"] = "unknown" if orders is None else ("pass" if not any(
        o["side"] == "BUY" and amount(o["remaining_size"]) > 0 and o["status"] not in {"CANCELED", "CANCELLED", "MATCHED"}
        for o in orders["rows"]) else "buy_present")
    if positions["current"] and assets["current"]:
        expected = {p["token_id"]: amount(p["size"]) for p in positions["current"]["rows"]}
        actual = {t: amount(v) for t, v in assets["current"]["holdings"].items()}
        checks["discovered_positions_reconciliation"] = "pass" if all(actual.get(t) == v for t, v in expected.items()) and all(expected.get(t, Decimal(0)) == v for t, v in actual.items()) else "mismatch"
    else:
        checks["discovered_positions_reconciliation"] = "unknown"
    passed = all(v in {"pass", "current"} for v in checks.values())
    return {"account_audit_passed": passed, "checks": checks, "observation": observation,
            "positions": positions, "chain_assets": assets, "principal_limit": str(principal),
            "live_enabled": False, "mutation_enabled": False, "budget_admission_enabled": False,
            "atomic_snapshot": False, "market_admission": "not_evaluated",
            "remaining_live_gates": ["fresh_executable_market_plan", "guardrail_acceptance",
                                     "explicit_live_authorization", "live_scoring_observation"],
            "limitations": ["trade_window_is_not_complete_fill_coverage", "fees_pnl_not_reconciled",
                            "public_position_discovery_is_not_exhaustive_inventory", "no_capital_journal_ingress"]}


def service_active():
    result = subprocess.run(["systemctl", "is-active", SERVICE_NAME], capture_output=True,
                            text=True, timeout=6, check=False)
    return result.returncode == 0 and result.stdout.strip() == "active"


def run_host(profile, *, tooling_sha=None, clock=utcnow):
    tooling = verify_tooling(tooling_sha)
    paths = aggressive_paths_for_profile(profile)
    release = _current_release(paths)
    release_dir = paths.release_root / release
    runtime_integrity = verify_runtime_release(release_dir, release)
    contract = _runtime_contract(paths, release_dir)
    # Never route aggressive acceptance through the stable signer or an arbitrary URL.
    require(contract["signer_url"] == "http://100.91.159.54:8421", "dedicated_signer_required")
    outputs, verifications = [], []
    for account in contract["local_accounts"]:
        reads, remote = None, None
        phase = "runtime_contract"
        try:
            config_path = paths.config_dir / f"config_{account.account_index}.json"
            state_path = paths.data_dir / f"engine_state_{account.account_index}.json"
            marker = paths.data_dir / f".account_{account.account_index}.paused"
            config = read_json(config_path, paths.runtime_root)
            identity = configured_identity(config)
            def verify(account=account, config=config, identity=identity, config_path=config_path,
                       state_path=state_path, marker=marker):
                require(_current_release(paths) == release, "release_changed_during_audit")
                current_contract = _runtime_contract(paths, release_dir)
                require(current_contract["roster_sha256"] == contract["roster_sha256"] and
                        current_contract["market_sha256"] == contract["market_sha256"], "contract_changed_during_audit")
                current_config = read_json(config_path, paths.runtime_root)
                require(configured_identity(current_config) == identity and
                        current_config.get("proxy_pool") == config.get("proxy_pool"), "config_changed_during_audit")
                require(market_universe_sha256(current_config) == market_universe_sha256(config),
                        "market_config_changed_during_audit")
                approved = None
                if market_universe_sha256(current_config) != current_contract["market_sha256"]:
                    approved = approved_market_config(paths, current_contract["market_sha256"])
                current_state = read_json(state_path, paths.runtime_root)
                active, checked_at = service_active(), clock()
                market_proof = check_runtime(account, current_config, current_state, contract, release,
                    paused_marker=marker.is_file() and marker.resolve().is_relative_to(paths.runtime_root.resolve()),
                    service_active=active, now=checked_at, approved_markets=approved)
                return {"state_ts": current_state["ts"], "checked_at": checked_at.isoformat(),
                        "market_config_integrity": market_proof}
            verify()
            proxy_items = [p for p in (config.get("proxy_pool") or {}).get("items", []) if p.get("enabled", True)]
            require(len(proxy_items) == 1, "one_account_proxy_required")
            proxy = proxy_items[0].get("url")
            require(proxy == f"http://127.0.0.1:{account.clash_port}", "isolated_account_proxy_required")
            remote = RemoteSignerClient(contract["signer_url"], token=contract["env"]["SIGNER_TOKEN"], funder=account.funder)
            phase = "existing_credentials"
            credentials = remote.derive_existing_creds()
            phase = "authenticated_reads"
            reads = AccountReads(identity, credentials, proxy)
            del credentials
            report = collect_acceptance(reads, identity, account.profile.target_principal_usdc, clock=clock)
            phase = "runtime_recheck"
            verify()
            report.update(runtime_paused_verified=True, account_index=account.account_index)
            outputs.append(report)
            verifications.append((report, verify))
        except AcceptanceError as exc:
            outputs.append({"account_index": account.account_index, "account_audit_passed": False, "reason": str(exc)})
        except Exception:
            outputs.append({"account_index": account.account_index, "account_audit_passed": False, "reason": phase + "_unavailable"})
        finally:
            if reads:
                reads.close()
            if remote:
                remote._session.close()
    for report, verify in verifications:
        try:
            report["runtime_final_check"] = verify()
        except AcceptanceError as exc:
            report.update(runtime_paused_verified=False, reason=str(exc))
        except Exception:
            report.update(runtime_paused_verified=False, reason="final_runtime_recheck_unavailable")
    require(verify_tooling(tooling_sha) == tooling, "tooling_changed_during_audit")
    require(verify_runtime_release(release_dir, release) == runtime_integrity,
            "runtime_integrity_changed_during_audit")
    finished = clock()
    # All accounts and sources are judged at the final host report timestamp.
    for report in outputs:
        if "observation" in report:
            report.update(assess_acceptance(report["observation"], report["positions"], report["chain_assets"],
                          amount(report["principal_limit"]), now=finished))
            evidence = report.get("runtime_final_check")
            fresh = bool(evidence) and 0 <= (finished-timestamp(evidence["state_ts"])).total_seconds() <= 60
            report["runtime_paused_verified"] = report["runtime_paused_verified"] and fresh
            report["checks"]["final_runtime"] = "pass" if report["runtime_paused_verified"] else "blocked"
            report["account_audit_passed"] = report["account_audit_passed"] and report["runtime_paused_verified"]
            if not fresh:
                report.setdefault("reason", "final_runtime_state_stale_or_unavailable")
    return {"kind": "aggressive_paused_account_acceptance", "schema_version": 1,
            "host_id": profile, "runtime_release_sha": release, "tooling_sha": tooling["commit"],
            "tooling_manifest_sha256": tooling["manifest_sha256"], "generated_at": finished.isoformat(),
            "runtime_integrity": runtime_integrity,
            "status": "pass" if outputs and all(o["account_audit_passed"] for o in outputs) else "blocked",
            "live_enabled": False, "mutation_enabled": False, "accounts": outputs}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, choices=("aggressive-a", "aggressive-b"))
    parser.add_argument("--tooling-sha", required=True, help="Reviewed full SHA of this immutable acceptance tool")
    args = parser.parse_args(argv)
    # CLI is a separate bounded process; do not expose SDK exception bodies/logs.
    previous_logging = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    def expired(*_):
        raise AcceptanceDeadline()
    previous_handler = signal.signal(signal.SIGALRM, expired)
    signal.alarm(240)
    try:
        result = run_host(args.profile, tooling_sha=args.tooling_sha)
    except AcceptanceDeadline:
        result = {"kind": "aggressive_paused_account_acceptance", "status": "blocked",
                  "reason": "acceptance_deadline_exceeded", "live_enabled": False, "mutation_enabled": False}
    except Exception:
        result = {"kind": "aggressive_paused_account_acceptance", "status": "blocked",
                  "reason": "host_contract_or_read_unavailable", "live_enabled": False, "mutation_enabled": False}
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)
        logging.disable(previous_logging)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
