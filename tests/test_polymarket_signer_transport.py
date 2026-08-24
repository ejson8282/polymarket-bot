from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "signer_server"))

from credential_transport import (  # noqa: E402
    CredentialTransportRecovery,
    is_request_transport_failure,
)


class RequestFailure(Exception):
    status_code = None

    def __str__(self) -> str:
        return "PolyApiException[status_code=None, error_message=Request exception!]"


class ApiFailure(Exception):
    status_code = 401

    def __str__(self) -> str:
        return "PolyApiException[status_code=401, error_message=Unauthorized]"


def test_request_transport_failure_is_narrow() -> None:
    assert is_request_transport_failure(RequestFailure()) is True
    assert is_request_transport_failure(ApiFailure()) is False
    assert is_request_transport_failure(RuntimeError("Request exception!")) is True
    assert is_request_transport_failure(RuntimeError("other")) is False


def test_transport_failure_resets_and_retries_once() -> None:
    resets = []
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RequestFailure()
        return "credentials"

    recovery = CredentialTransportRecovery(lambda: resets.append("reset"))

    result, recovered = recovery.run(operation)

    assert result == "credentials"
    assert recovered is True
    assert attempts == 2
    assert resets == ["reset"]


def test_non_transport_failure_does_not_reset_or_retry() -> None:
    resets = []
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        raise ApiFailure()

    recovery = CredentialTransportRecovery(lambda: resets.append("reset"))

    with pytest.raises(ApiFailure):
        recovery.run(operation)

    assert attempts == 1
    assert resets == []


def test_retry_failure_is_returned_without_a_second_reset() -> None:
    resets = []
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        raise RequestFailure()

    recovery = CredentialTransportRecovery(lambda: resets.append("reset"))

    with pytest.raises(RequestFailure):
        recovery.run(operation)

    assert attempts == 2
    assert resets == ["reset"]


def test_ready_actively_checks_accounts_without_returning_secrets(monkeypatch) -> None:
    import signer_server

    monkeypatch.setattr(
        signer_server,
        "KEY_MAP",
        {"0xfunder-one": "0xprivate-one", "0xfunder-two": "0xprivate-two"},
    )
    monkeypatch.setattr(signer_server, "BEARER_TOKEN", "test-token")
    monkeypatch.setattr(signer_server, "ALLOWED_IPS", [])
    monkeypatch.setattr(
        signer_server,
        "_get_credentials_client",
        lambda funder: SimpleNamespace(funder=funder),
    )
    monkeypatch.setattr(
        signer_server,
        "_derive_credentials",
        lambda client: (SimpleNamespace(api_secret="must-not-leak"), False),
    )
    signer_server._readiness.clear()

    response = TestClient(signer_server.app).post(
        "/ready",
        headers={"Authorization": "Bearer test-token"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "funders_configured": 2,
        "funders_ready": 2,
        "results": [
            {"account": 1, "ok": True, "transport_recovered": False},
            {"account": 2, "ok": True, "transport_recovered": False},
        ],
    }
    encoded = response.text
    assert "funder-one" not in encoded
    assert "private-one" not in encoded
    assert "must-not-leak" not in encoded
