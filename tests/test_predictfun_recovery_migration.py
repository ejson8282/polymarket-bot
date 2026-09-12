from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from platforms.predictfun.maker.executor import ExecutableOrder, ExecutionResult
from platforms.predictfun.maker.managed_orders import ManagedOrderRegistry
from platforms.predictfun.maker import recovery_migration as migration
from tests.test_predictfun_recovery_plan import inputs

VERIFY_WRITERS = migration.verify_vps1_writers_stopped


def proposal():
    args = inputs()
    pending = args["managed_state"]["pending_submissions"][0]
    pending.update(intent_id="old", outcome="YES", side="BUY", price="0.1", size="1")
    args["managed_state"]["summary"] = {"pending_submissions": 2, "active": 0}
    args["managed_state"]["pending_submissions"].append({
        **pending, "account_id": "account_02", "unknown_field": "preserve"})
    args["managed_state"]["submission_generations"]["account_02"] = {"old": 20}
    args["managed_state"]["orders"] = [{"order_id": "keep", "intent_id": "other",
        "account_id": "account_02", "status": "cancelled", "created_at": "then",
        "updated_at": "then", "market_id": 9}]
    report = {"managed_orders": args.pop("managed_state"), "risk": {"paused": True},
              "untouched": {"unknown": [1, 2, 3]}}
    before = deepcopy(report)
    result = migration.prepare_report_recovery(report, **args)
    assert report == before
    return report, result


def test_preserves_history_other_account_and_advances_only_selected_generation():
    original, result = proposal()
    state = result["replacement"]["managed_orders"]
    assert result["activation_allowed"] is False
    assert state["orders"] == original["managed_orders"]["orders"]
    assert state["submission_generations"] == {"account_01": {"old": 2}, "account_02": {"old": 20}}
    assert state["pending_submissions"] == original["managed_orders"]["pending_submissions"][1:]
    assert state["recovery_archive"][0]["pending"] == original["managed_orders"]["pending_submissions"][0]
    assert state["recovery_archive"][0]["historical_submission_outcome"] == "unknown"
    assert result["replacement"]["risk"] == original["risk"]


def test_archive_survives_runner_roundtrip_and_history_trimming():
    _, result = proposal()
    state = result["replacement"]["managed_orders"]
    registry = ManagedOrderRegistry.from_state(state, history_limit=1)
    for _ in range(3):
        registry._trim()
        registry = ManagedOrderRegistry.from_state(registry.to_state(), history_limit=1)
        assert registry.idempotency_key_for_create("old", "account_01") == "old:g3"
        assert registry.idempotency_key_for_create("old", "account_02") == "old:g21"
        assert registry.to_state()["recovery_archive"] == state["recovery_archive"]
    persisted = registry.to_state()
    persisted["submission_generations"] = {}
    rebuilt = ManagedOrderRegistry.from_state(persisted)
    assert rebuilt.idempotency_key_for_create("old", "account_01") == "old:g3"


def test_archived_key_refuses_reentry_even_with_successful_response():
    _, result = proposal()
    registry = ManagedOrderRegistry.from_state(result["replacement"]["managed_orders"])
    order = ExecutableOrder(intent_id="old", account_id="account_01", market_id=42,
        outcome="YES", side="BUY", price=Decimal("0.1"), size=Decimal("1"),
        idempotency_key="old:g2")
    with pytest.raises(ValueError, match="replay"):
        registry.record_submission_pending(order)
    with pytest.raises(ValueError, match="replay"):
        registry.record_create(order, ExecutionResult(ok=True, order_id="new", status="open",
            intent_id="old", account_id="account_01", action="create", message="test"))


@pytest.mark.parametrize("broken", [None, {}, "old", [{"pending": {}}]])
def test_bad_archive_fail_closed(broken):
    _, result = proposal()
    state = result["replacement"]["managed_orders"]
    if broken is None:
        state["recovery_archive"][0]["pending"]["idempotency_key"] = "old:g01"
    else:
        state["recovery_archive"] = broken
    with pytest.raises(ValueError, match="recovery"):
        ManagedOrderRegistry.from_state(state)


def test_archive_pending_conflict_not_silently_discarded():
    original, result = proposal()
    state = result["replacement"]["managed_orders"]
    state["pending_submissions"].append(original["managed_orders"]["pending_submissions"][0])
    with pytest.raises(ValueError, match="still_pending"):
        ManagedOrderRegistry.from_state(state)


@pytest.fixture(autouse=True)
def isolated_runtime_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(migration, "VPS1_REPORT", tmp_path / "runtime" / "predictfun_mainnet_execution_report.json")
    monkeypatch.setattr(migration, "VPS1_DEPLOY_LOCK", tmp_path / "locks" / "deploy.lock")
    monkeypatch.setattr(migration, "verify_vps1_writers_stopped", lambda _: None)


def setup_apply(tmp_path):
    original, planned = proposal()
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    target = runtime / "predictfun_mainnet_execution_report.json"
    target.write_bytes(migration._bytes(original))
    backups = tmp_path / "backups"
    backups.mkdir()
    calls = []
    def verify():
        calls.append("verified")
    args = dict(target=target, backup_root=backups, proposal=planned,
                expected_file_sha256=migration._sha(target.read_bytes()),
                reviewed_proposal_sha256=migration._digest(planned),
                release_sha="a" * 40, authorization_id="unit-test-only",
                verify_maintenance=verify)
    return args, calls


def test_locked_replacement_with_verifiable_private_backup(tmp_path):
    args, calls = setup_apply(tmp_path)
    original = args["target"].read_bytes()
    result = migration.replace_reviewed_report(**args)
    assert calls == ["verified", "verified"]
    assert result["activation_allowed"] is False
    assert result["automatic_rollback_allowed"] is False
    assert result["status"] == "applied_keep_paused"
    folder = Path(result["backup_directory"])
    assert (folder / "original.json").read_bytes() == original
    assert (folder / "replacement.json").read_bytes() == args["target"].read_bytes()
    assert (folder / "applied.json").is_file()
    for path in folder.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    assert args["target"].stat().st_mode & 0o777 == 0o600


def test_active_runner_lock_blocks_any_backup_or_write(tmp_path):
    args, calls = setup_apply(tmp_path)
    before = args["target"].read_bytes()
    with migration.report_writer_lease(args["target"]):
        with pytest.raises(ValueError, match="writer_active"):
            migration.replace_reviewed_report(**args)
    assert calls == []
    assert args["target"].read_bytes() == before
    assert list(args["backup_root"].iterdir()) == []


@pytest.mark.parametrize("stage", [1, 2])
def test_failed_live_guard_never_replaces_report(tmp_path, stage):
    args, _ = setup_apply(tmp_path)
    before = args["target"].read_bytes()
    count = 0
    def verify():
        nonlocal count
        count += 1
        if count == stage:
            raise ValueError("fence_or_release_or_nonce_unverified")
    args["verify_maintenance"] = verify
    with pytest.raises(ValueError, match="unverified"):
        migration.replace_reviewed_report(**args)
    assert args["target"].read_bytes() == before
    assert not list(args["target"].parent.glob(".predict-recovery-*"))


def test_concurrent_noncooperating_write_detected(tmp_path):
    args, _ = setup_apply(tmp_path)
    calls = 0
    def verify():
        nonlocal calls
        calls += 1
        if calls == 2:
            args["target"].write_text('{"external_change": true}')
    args["verify_maintenance"] = verify
    with pytest.raises(ValueError, match="changed_during"):
        migration.replace_reviewed_report(**args)
    assert json.loads(args["target"].read_text()) == {"external_change": True}


@pytest.mark.parametrize("field,value", [("authorization_id", ""), ("release_sha", "short"),
    ("reviewed_proposal_sha256", "b" * 64), ("expected_file_sha256", "b" * 64)])
def test_wrong_scope_or_unreviewed_hash_refused(tmp_path, field, value):
    args, _ = setup_apply(tmp_path)
    before = args["target"].read_bytes()
    args[field] = value
    with pytest.raises(ValueError):
        migration.replace_reviewed_report(**args)
    assert args["target"].read_bytes() == before


def test_backup_failure_never_replaces_report(tmp_path, monkeypatch):
    args, _ = setup_apply(tmp_path)
    before = args["target"].read_bytes()
    monkeypatch.setattr(migration, "_write_new", lambda *a: (_ for _ in ()).throw(OSError("disk_full")))
    with pytest.raises(OSError, match="disk_full"):
        migration.replace_reviewed_report(**args)
    assert args["target"].read_bytes() == before


def test_review_hash_cannot_authorize_changes_to_other_account_or_risk(tmp_path):
    args, _ = setup_apply(tmp_path)
    before = args["target"].read_bytes()
    args["proposal"]["replacement"]["risk"]["paused"] = False
    args["reviewed_proposal_sha256"] = migration._digest(args["proposal"])
    with pytest.raises(ValueError, match="outside_recovery_scope"):
        migration.replace_reviewed_report(**args)
    assert args["target"].read_bytes() == before


def test_runner_acquires_lease_before_trading_and_releases_on_error(tmp_path, monkeypatch):
    from platforms.predictfun.maker import runner
    config = tmp_path / "config.json"
    target = tmp_path / "report.json"
    reads = []
    cfg = {"output": {"execution_report_path": str(target)}}
    def load(_):
        reads.append("read")
        assert len(reads) == 1, "writer lease and runner must use one config snapshot"
        return cfg
    monkeypatch.setattr(runner, "load_config", load)
    def run(**kwargs):
        assert kwargs["cfg"] is cfg
        with pytest.raises(ValueError, match="writer_active"):
            with migration.report_writer_lease(target):
                pytest.fail("lease must remain held through shutdown")
        raise ValueError("runner_failure")
    monkeypatch.setattr(runner, "_run_loop_locked", run)
    with pytest.raises(ValueError, match="runner_failure"):
        runner.run_loop(config_path=config, interval_sec=1, once=True)
    with migration.report_writer_lease(target):
        pass


@pytest.mark.parametrize("active,masked,pid,manual,accepted", [
    ("inactive", "masked-runtime", "0", False, True),
    ("inactive", "disabled", "0", False, False),
    ("active", "masked-runtime", "0", False, False),
    ("inactive", "masked-runtime", "123", False, False),
    ("inactive", "masked-runtime", "0", True, False),
])
def test_actual_writer_probe_requires_mask_and_no_process(monkeypatch, active, masked, pid, manual, accepted):
    monkeypatch.setattr(migration.Path, "resolve", lambda *a, **k: Path("/release/" + "a" * 40))
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        if argv[0] == "ip":
            return SimpleNamespace(stdout=json.dumps([{"ifname": "tailscale0", "addr_info": [{"local": "100.122.255.98"}]}]))
        if argv[0] == "ps":
            return SimpleNamespace(stdout="python platforms.predictfun.maker.runner" if manual else "sshd")
        assert argv[:2] == ["systemctl", "show"]
        return SimpleNamespace(stdout=f"Id={argv[2]}\nLoadState=masked\nActiveState={active}\n"
            f"UnitFileState={masked}\nMainPID={pid}\nControlPID=0\n")
    monkeypatch.setattr(migration.subprocess, "run", run)
    if accepted:
        VERIFY_WRITERS("a" * 40)
        assert len(calls) == 4
    else:
        with pytest.raises(ValueError):
            VERIFY_WRITERS("a" * 40)
    assert all(argv[0] in {"ps", "ip"} or argv[1] == "show" for argv in calls)


def test_old_release_refused_before_process_probe(monkeypatch):
    monkeypatch.setattr(migration.Path, "resolve", lambda *a, **k: Path("/release/" + "b" * 40))
    monkeypatch.setattr(migration.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout=json.dumps([
        {"ifname": "tailscale0", "addr_info": [{"local": "100.122.255.98"}]}])))
    with pytest.raises(ValueError, match="unexpected_predict_release"):
        VERIFY_WRITERS("a" * 40)


def test_other_host_refused_before_service_reads(monkeypatch):
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(stdout=json.dumps([{"ifname": "tailscale0", "addr_info": [
            {"local": "100.101.50.40"}]}]))
    monkeypatch.setattr(migration.subprocess, "run", run)
    with pytest.raises(ValueError, match="not_vps1"):
        VERIFY_WRITERS("a" * 40)
    assert len(calls) == 1


def test_global_deployment_lock_excludes_recovery(tmp_path):
    args, calls = setup_apply(tmp_path)
    with migration._exclusive_lease(migration.VPS1_DEPLOY_LOCK):
        with pytest.raises(ValueError, match="writer_active"):
            migration.replace_reviewed_report(**args)
    assert calls == []
    assert list(args["backup_root"].iterdir()) == []
