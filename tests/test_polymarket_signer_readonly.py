"""Use synthetic credentials only. No signer environment or live keys loaded."""
from pathlib import Path
import sys
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "signer_server"))
from credential_transport import CredentialTransportRecovery
from platforms.polymarket.maker.remote_signer import RemoteSignerClient


FUNDER = "0x" + "a" * 40
SIGNER = "0x" + "b" * 40
CREDS = SimpleNamespace(api_key="test-key", api_secret="test-secret", api_passphrase="test-pass")


@pytest.fixture
def server(monkeypatch):
    for key in ("SIGNER_KEYS_JSON", "POLY_PRIVATE_KEY", "POLY_FUNDER", "SIGNER_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    import signer_server as mod
    monkeypatch.setattr(mod, "KEY_MAP", {FUNDER: "synthetic-not-a-key"})
    monkeypatch.setattr(mod, "BEARER_TOKEN", "test-bearer")
    monkeypatch.setattr(mod, "ALLOWED_IPS", ["testclient"])
    monkeypatch.setattr(mod, "HOST", "https://clob.polymarket.com")
    monkeypatch.setattr(mod, "CHAIN_ID", 137)
    monkeypatch.setattr(mod, "SIGNATURE_TYPE", 2)
    monkeypatch.setattr(mod, "_locked", False)
    monkeypatch.setattr(mod, "_request_timestamps", [])
    monkeypatch.setattr(mod, "_credential_transport_recovery", CredentialTransportRecovery(lambda: None))
    def forbidden(*args, **kwargs):
        pytest.fail("credential creation or order signing path reached")
    fake = SimpleNamespace(derive_api_key=lambda: CREDS, get_address=lambda: SIGNER,
                           create_api_key=forbidden, create_or_derive_api_creds=forbidden)
    monkeypatch.setattr(mod, "_get_credentials_client", lambda funder: fake)
    monkeypatch.setattr(mod, "_get_client", forbidden)
    with TestClient(mod.app) as client:
        yield mod, fake, client


def request(client, body=None, token="test-bearer"):
    return client.post("/derive-existing-creds", json=body if body is not None else {"funder": FUNDER},
                       headers={"Authorization": f"Bearer {token}"})


def test_existing_route_returns_matching_identity_without_creating(server):
    _, _, client = server
    response = request(client)
    assert response.status_code == 200
    assert response.json() == {**vars(CREDS), "address": SIGNER, "mode": "existing_only",
                               "funder": FUNDER, "chain_id": 137, "signature_type": 2}


@pytest.mark.parametrize("body,code", [({}, 422), ({"funder": ""}, 422),
                                     ({"funder": SIGNER}, 404), ({"funder": None}, 422)])
def test_explicit_known_funder_required(server, body, code):
    _, _, client = server
    assert request(client, body).status_code == code


def test_auth_ip_lock_and_rate_limits_preserved(server, monkeypatch):
    mod, _, client = server
    assert request(client, token="wrong").status_code == 401
    monkeypatch.setattr(mod, "ALLOWED_IPS", ["other"])
    assert request(client).status_code == 403
    monkeypatch.setattr(mod, "ALLOWED_IPS", ["testclient"])
    monkeypatch.setattr(mod, "_locked", True)
    assert request(client).status_code == 423
    monkeypatch.setattr(mod, "_locked", False)
    monkeypatch.setattr(mod, "MAX_REQUESTS_PER_MINUTE", 0)
    assert request(client).status_code == 429


def test_no_legacy_funder_fallback(server, monkeypatch):
    mod, _, client = server
    monkeypatch.setattr(mod, "KEY_MAP", {"": "synthetic"})
    assert request(client).status_code == 404


@pytest.mark.parametrize("value", [None, SimpleNamespace(api_key="", api_secret="x", api_passphrase="y")])
def test_missing_credentials_fail_closed(server, value):
    _, fake, client = server
    fake.derive_api_key = lambda: value
    assert request(client).status_code == 503


def test_secret_in_exception_not_in_response_or_logs(server, caplog):
    _, fake, client = server
    def fail():
        raise RuntimeError("test-secret test-pass RAW-UPSTREAM-SECRET")
    fake.derive_api_key = fail
    response = request(client)
    assert response.status_code == 503
    assert response.json()["detail"] == "existing_credentials_unavailable"
    assert "RAW-UPSTREAM-SECRET" not in response.text + caplog.text
    assert "test-secret" not in response.text + caplog.text


def test_recovery_retries_derive_only(server):
    _, fake, client = server
    calls = []
    def derive():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("Request exception!")
        return CREDS
    fake.derive_api_key = derive
    assert request(client).status_code == 200
    assert len(calls) == 2


def test_legacy_route_keeps_existing_create_or_derive_behavior(server):
    _, fake, client = server
    calls = []
    fake.create_or_derive_api_creds = lambda: calls.append("legacy") or CREDS
    response = client.post("/derive-creds", json={"funder": FUNDER},
                           headers={"Authorization": "Bearer test-bearer"})
    assert response.status_code == 200
    assert calls == ["legacy"]


def test_installed_standard_sdk_derivation_is_get_only(monkeypatch):
    import py_clob_client.client as sdk
    calls = []
    client = object.__new__(sdk.ClobClient)
    client.host = "https://clob.polymarket.com"
    client.signer = object()
    client.assert_level_1_auth = lambda: None
    monkeypatch.setattr(sdk, "create_level_1_headers", lambda *a: {"synthetic": "header"})
    monkeypatch.setattr(sdk, "get", lambda url, **kw: calls.append(url) or
                        {"apiKey": "test-key", "secret": "test-secret", "passphrase": "test-pass"})
    monkeypatch.setattr(sdk, "post", lambda *a, **kw: pytest.fail("upstream POST forbidden"))
    assert client.derive_api_key().api_key == "test-key"
    assert calls == ["https://clob.polymarket.com/auth/derive-api-key"]


def test_remote_method_contract_and_no_redirects(monkeypatch):
    import requests
    remote = RemoteSignerClient("http://127.0.0.1:8421", "test-bearer", funder=FUNDER)
    calls = []
    payload = {**vars(CREDS), "address": SIGNER, "mode": "existing_only", "funder": FUNDER,
               "chain_id": 137, "signature_type": 2}
    class Session:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def post(self, url, **kwargs):
            calls.append((url, kwargs))
            return SimpleNamespace(status_code=200, json=lambda: payload)
    monkeypatch.setattr(requests, "Session", Session)
    assert remote.derive_existing_creds()["funder"] == FUNDER
    assert calls[0][0].endswith("/derive-existing-creds")
    assert calls[0][1]["allow_redirects"] is False
    assert calls[0][1]["timeout"] == (5, 15.0)
    for field, value in (("funder", SIGNER), ("mode", "create_or_derive"), ("chain_id", True),
                         ("api_secret", ""), ("signature_type", True)):
        original = payload[field]
        payload[field] = value
        with pytest.raises(ValueError, match="existing_credentials_unavailable_or_invalid"):
            remote.derive_existing_creds()
        payload[field] = original
    remote._session.close()
