import ast
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional


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


def _compiled_module_function(name: str):
    function = _module_function(name)
    namespace = {"Any": Any}
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    exec(compile(module, "<discord-test>", "exec"), namespace)
    return namespace[name]


def _compiled_engine_method(name: str):
    function = _engine_method(name)
    namespace = {"Decimal": Decimal, "Optional": Optional}
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    exec(compile(module, "<discord-test>", "exec"), namespace)
    return namespace[name]


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


def test_structured_discord_payload_is_never_rendered_as_raw_json() -> None:
    render = _compiled_module_function("_discord_description")

    description = render({"原因": "盘口变化", "数量": 2})

    assert description == "原因：盘口变化\n数量：2"
    assert "{" not in description
    assert '"原因"' not in description


def test_fill_alert_is_concise_chinese_without_orderbook_dump() -> None:
    format_fill = _compiled_engine_method("_format_fill_alert")

    class FakeEngine:
        @staticmethod
        def _discord_market_name(_token_id: str) -> str:
            return "示例市场"

        @staticmethod
        def _discord_reason(_reason: str) -> str:
            return "WebSocket 成交回报"

    message = format_fill(
        FakeEngine(),
        "token-1",
        "WS_TRADE_MATCH:12.34",
        Decimal("12.34"),
        Decimal("0.78"),
    )

    assert "市场：示例市场" in message
    assert "成交数量：12.34 份" in message
    assert "成交价格：$0.7800（金额 $9.63）" in message
    assert "系统处理：已撤销相关买单，正在退出仓位" in message
    assert "ORDERBOOK" not in message
    assert "ASK" not in message
    assert "BID" not in message
    assert "token-1" not in message


def test_key_operator_notifications_use_chinese_titles_and_no_dict_payload() -> None:
    engine_path = (
        Path(__file__).resolve().parents[1]
        / "platforms"
        / "polymarket"
        / "maker"
        / "engine.py"
    )
    source = engine_path.read_text(encoding="utf-8")

    for title in (
        '"检测到成交"',
        '"可用余额下降"',
        '"退出单已提交"',
        '"对侧风险保护已触发"',
        '"安全暂停已触发"',
    ):
        assert title in source

    assert '"Fill Detected"' not in source
    assert '"Balance Drop"' not in source
    assert '"Exit Sell Placed"' not in source
    assert '"🛡 cross-side sentinel triggered"' not in source
