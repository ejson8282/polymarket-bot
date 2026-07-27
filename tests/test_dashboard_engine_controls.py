from pathlib import Path


APP_SOURCE = (
    Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
).read_text(encoding="utf-8")


def test_stop_is_non_blocking_and_clears_optimistic_start_state():
    assert 'st.session_state.pop("_engine_just_started_at", None)' in APP_SOURCE
    assert (
        '["sudo", "-n", "systemctl", "--no-block", "stop", LOCAL_ENGINE_UNIT]'
        in APP_SOURCE
    )


def test_stop_uses_async_cancel_fallback():
    assert 'cancel_cli = MAKER_DIR / "cancel_all_cli.py"' in APP_SOURCE
    assert "subprocess.Popen(" in APP_SOURCE
    assert "start_new_session=True" in APP_SOURCE


def test_remote_start_stop_are_non_blocking():
    assert 'if action in {"start", "stop"}:' in APP_SOURCE
    assert 'systemctl_args.append("--no-block")' in APP_SOURCE
