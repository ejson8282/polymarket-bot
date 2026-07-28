import ast
from pathlib import Path


def _message_classifier():
    engine_path = (
        Path(__file__).resolve().parents[1]
        / "platforms"
        / "polymarket"
        / "maker"
        / "engine.py"
    )
    tree = ast.parse(engine_path.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_is_important_discord_message"
    )
    namespace: dict[str, object] = {}
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    exec(compile(module, str(engine_path), "exec"), namespace)
    return namespace["_is_important_discord_message"]


def _engine_method(name: str) -> ast.FunctionDef:
    engine_path = (
        Path(__file__).resolve().parents[1]
        / "platforms"
        / "polymarket"
        / "maker"
        / "engine.py"
    )
    tree = ast.parse(engine_path.read_text(encoding="utf-8"))
    engine_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "PolyLPSMulti"
    )
    return next(
        node
        for node in engine_class.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def _module_function(name: str) -> ast.FunctionDef:
    engine_path = (
        Path(__file__).resolve().parents[1]
        / "platforms"
        / "polymarket"
        / "maker"
        / "engine.py"
    )
    tree = ast.parse(engine_path.read_text(encoding="utf-8"))
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def test_polymarket_discord_message_classification() -> None:
    classify = _message_classifier()

    assert classify("[EXIT FAILED] unable to close position")
    assert classify("资金不足，需要人工处理")
    assert classify("dust remains after close")
    assert not classify("[挂单] order placed")
    assert not classify("[成交] position opened")


def test_polymarket_fill_messages_use_normal_router() -> None:
    method = _engine_method("send_fill_discord")
    names = {
        node.attr
        for node in ast.walk(method)
        if isinstance(node, ast.Attribute)
    }

    assert "send_discord" in names
    assert "fill_discord_webhook" not in names


def test_polymarket_notifications_reload_dashboard_webhook_files() -> None:
    function = _module_function("_discord_webhook_for")
    names = {
        node.id
        for node in ast.walk(function)
        if isinstance(node, ast.Name)
    }
    engine_path = (
        Path(__file__).resolve().parents[1]
        / "platforms"
        / "polymarket"
        / "maker"
        / "engine.py"
    )
    source = engine_path.read_text(encoding="utf-8")

    assert "_discord_normal_webhook_file" in names
    assert "_discord_important_webhook_file" in names
    assert "discord_normal_webhook.txt" in source
    assert "discord_important_webhook.txt" in source


def test_polymarket_has_no_independent_webhook_route() -> None:
    method = _engine_method("send_discord")
    names = {
        node.id
        for node in ast.walk(method)
        if isinstance(node, ast.Name)
    }
    engine_path = (
        Path(__file__).resolve().parents[1]
        / "platforms"
        / "polymarket"
        / "maker"
        / "engine.py"
    )
    source = engine_path.read_text(encoding="utf-8")

    assert "_discord_webhook_for" in names
    for legacy_key in (
        "POLY_DISCORD_WEBHOOK",
        "POLY_FILL_DISCORD_WEBHOOK",
        "POLY_IMPORTANT_DISCORD_WEBHOOK",
        "discord_webhook_url",
        'reporting.get("discord_webhook"',
        'reporting.get("fill_discord_webhook"',
    ):
        assert legacy_key not in source
