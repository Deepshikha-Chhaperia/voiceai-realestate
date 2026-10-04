import base64
import hashlib
import hmac
import os
import tempfile

os.environ["DATA_DIR"] = tempfile.mkdtemp()  # before importing main: lead_state reads it at import

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import main
from settings import validate_startup

STRONG = "x" * 32
CONFIG_VARS = (
    "LOCAL_DEMO", "HOST", "WS_TOKEN", "DASHBOARD_API_KEY", "PUBLIC_HOST", "FORCE_WSS",
    "VOBIZ_AUTH_TOKEN", "VERIFY_VOBIZ_SIGNATURE", "OUTBOUND_API_KEY", "REDIS_URL", "DATABASE_URL",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    # bot.py calls load_dotenv(override=True) at import, so scrub whatever a real .env injected
    for name in CONFIG_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def client():
    return TestClient(main.app)  # no `with`: skips lifespan (provider warm-up), routes still work


def prod_env(monkeypatch):
    monkeypatch.setenv("WS_TOKEN", STRONG)
    monkeypatch.setenv("DASHBOARD_API_KEY", STRONG)
    monkeypatch.setenv("PUBLIC_HOST", "bot.example.com")


def test_prod_requires_three_vars(monkeypatch):  # (a)
    with pytest.raises(RuntimeError):
        validate_startup()
    monkeypatch.setenv("WS_TOKEN", "short")  # too short counts as failure too
    monkeypatch.setenv("DASHBOARD_API_KEY", STRONG)
    monkeypatch.setenv("PUBLIC_HOST", "bot.example.com")
    with pytest.raises(RuntimeError):
        validate_startup()


def test_prod_passes_with_all_three(monkeypatch):  # (b)
    prod_env(monkeypatch)
    validate_startup()


def test_local_demo_refuses_non_loopback_host(monkeypatch):  # (c)
    monkeypatch.setenv("LOCAL_DEMO", "1")
    monkeypatch.setenv("HOST", "0.0.0.0")
    with pytest.raises(RuntimeError):
        validate_startup()


def test_local_demo_generates_secrets_and_ws_rejects_without_token(monkeypatch, client):  # (d)
    monkeypatch.setenv("LOCAL_DEMO", "1")
    validate_startup()
    assert len(os.environ["WS_TOKEN"]) >= 24 and len(os.environ["DASHBOARD_API_KEY"]) >= 24
    assert os.environ["PUBLIC_HOST"] == "localhost:8000"
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws"):
            pass
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws?token=wrong"):
            pass


def test_ws_fails_closed_when_token_unset(client):
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws"):
            pass


def test_dashboard_denied_without_key_even_in_local_demo(monkeypatch, client):  # (e)
    monkeypatch.setenv("LOCAL_DEMO", "1")
    validate_startup()
    assert client.get("/dashboard").status_code == 401
    assert client.get("/calls").status_code == 401
    monkeypatch.setenv("DASHBOARD_API_KEY", "")  # empty key must deny, not open
    assert client.get("/dashboard").status_code == 401
    assert client.get("/calls", headers={"X-API-Key": ""}).status_code == 401


@pytest.mark.parametrize("method,path", [("get", "/lp"), ("post", "/webhooks/website"), ("get", "/webhooks/meta"), ("post", "/webhooks/meta")])
def test_dead_routes_are_gone(client, method, path):  # (f)
    assert getattr(client, method)(path).status_code == 404


def test_inbound_rejects_invalid_signature(monkeypatch, client):  # (g)
    prod_env(monkeypatch)
    monkeypatch.setenv("VOBIZ_AUTH_TOKEN", "vobiz-secret")
    assert client.post("/inbound").status_code == 401
    bad = {"X-Vobiz-Signature-V2": "AAAA", "X-Vobiz-Signature-V2-Nonce": "n1"}
    assert client.post("/inbound", headers=bad).status_code == 401
    assert client.post("/outbound-answer?call_id=1", headers=bad).status_code == 401


def test_inbound_accepts_valid_signature_and_uses_public_host(monkeypatch, client):
    prod_env(monkeypatch)
    monkeypatch.setenv("VOBIZ_AUTH_TOKEN", "vobiz-secret")
    monkeypatch.setenv("FORCE_WSS", "true")
    nonce = "n1"
    digest = hmac.new(b"vobiz-secret", f"https://bot.example.com/inbound{nonce}".encode(), hashlib.sha256).digest()
    headers = {
        "X-Vobiz-Signature-V2": base64.b64encode(digest).decode(),
        "X-Vobiz-Signature-V2-Nonce": nonce,
        "Host": "attacker.example",
    }
    r = client.post("/inbound", headers=headers)
    assert r.status_code == 200
    assert f"wss://bot.example.com/ws?token={STRONG}" in r.text
    assert "attacker.example" not in r.text


def test_signature_check_can_be_disabled_by_flag(monkeypatch, client):
    prod_env(monkeypatch)
    monkeypatch.setenv("VERIFY_VOBIZ_SIGNATURE", "false")
    assert client.post("/inbound").status_code == 200


def test_outbound_fails_closed_without_key(monkeypatch, client):
    body = {"to": "+919876543210"}
    assert client.post("/outbound", json=body).status_code == 500  # OUTBOUND_API_KEY unset
    monkeypatch.setenv("OUTBOUND_API_KEY", STRONG)
    assert client.post("/outbound", json=body).status_code == 401
    assert client.post("/outbound", json=body, headers={"X-API-Key": "wrong"}).status_code == 401


def test_health_exposes_only_status(client):
    assert client.get("/health").json() == {"status": "healthy"}


def test_non_ascii_credentials_are_401_not_500(monkeypatch, client):
    prod_env(monkeypatch)
    monkeypatch.setenv("OUTBOUND_API_KEY", STRONG)
    monkeypatch.setenv("VOBIZ_AUTH_TOKEN", "vobiz-secret")
    bad = {"X-API-Key": b"\xe9"}
    assert client.get("/calls", headers=bad).status_code == 401
    assert client.post("/outbound", json={"to": "+919876543210"}, headers=bad).status_code == 401
    assert client.get("/dashboard", headers={"Authorization": b"\xe9"}).status_code == 401
    assert client.post("/inbound", headers={"X-Vobiz-Signature-V2": b"\xe9", "X-Vobiz-Signature-V2-Nonce": "n"}).status_code == 401
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws?token=%C3%A9"):
            pass


def test_outbound_answer_call_id_cannot_inject_xml(monkeypatch, client):
    prod_env(monkeypatch)
    monkeypatch.setenv("VERIFY_VOBIZ_SIGNATURE", "false")
    r = client.post("/outbound-answer?call_id=1%22%3C/Stream%3E%3CHangup/%3E")
    assert r.status_code == 200 and "<Hangup/>" not in r.text

