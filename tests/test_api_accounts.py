"""Tests for the /accounts endpoint — cookie import formats and validation."""

from __future__ import annotations

import json
import os

import pytest
from fastapi.testclient import TestClient

from libs.core import crypto


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    """Use a temp DB and reset crypto warning flag."""
    monkeypatch.setenv("DESEARCH_DB_PATH", str(tmp_path / "test.sqlite"))
    monkeypatch.delenv("DESEARCH_ENCRYPTION_KEY", raising=False)
    crypto._warned_no_key = False


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DESEARCH_DB_PATH", str(tmp_path / "test.sqlite"))
    # Re-import to pick up fresh Storage with tmp db
    # We patch Storage init to use tmp_path
    from libs.core.storage import Storage

    storage = Storage(db_path=tmp_path / "test.sqlite")
    storage.migrate()

    from apps.api.main import app

    import apps.api.main as api_mod

    original_storage = api_mod.storage
    api_mod.storage = storage
    yield TestClient(app)
    api_mod.storage = original_storage
    storage.close()


class TestCreateAccountRawFields:
    def test_create_with_li_at(self, client):
        resp = client.post(
            "/accounts",
            json={"label": "test", "li_at": "AQEDAWx0Y29va2llXXX"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "account_id" in data
        assert isinstance(data["account_id"], int)

    def test_create_with_li_at_and_jsessionid(self, client):
        resp = client.post(
            "/accounts",
            json={"label": "test", "li_at": "AQEDAWx0Y29va2llXXX", "jsessionid": "ajax:tok123"},
        )
        assert resp.status_code == 200

    def test_missing_auth_rejected(self, client):
        resp = client.post("/accounts", json={"label": "test"})
        assert resp.status_code == 422


class TestCreateAccountCookieString:
    def test_create_with_cookies_string(self, client):
        resp = client.post(
            "/accounts",
            json={"label": "test", "cookies": "li_at=AQEDAWx0Y29va2llXXX; JSESSIONID=ajax:tok123"},
        )
        assert resp.status_code == 200
        assert "account_id" in resp.json()

    def test_cookies_string_without_li_at_rejected(self, client):
        resp = client.post(
            "/accounts",
            json={"label": "test", "cookies": "JSESSIONID=ajax:tok123"},
        )
        assert resp.status_code == 422

    def test_cookies_overrides_raw_fields(self, client):
        resp = client.post(
            "/accounts",
            json={
                "label": "test",
                "li_at": "should_be_ignored",
                "cookies": "li_at=AQEDAWx0Y29va2llXXX",
            },
        )
        assert resp.status_code == 200


class TestCreateAccountValidation:
    def test_short_li_at_rejected(self, client):
        resp = client.post("/accounts", json={"label": "test", "li_at": "abc"})
        assert resp.status_code == 422

    def test_empty_li_at_rejected(self, client):
        resp = client.post("/accounts", json={"label": "test", "li_at": ""})
        assert resp.status_code == 422


class TestRefreshAccount:
    def test_refresh_updates_auth(self, client):
        create = client.post(
            "/accounts",
            json={"label": "r1", "li_at": "AQEDAWx0Y29va2llXXX", "jsessionid": "ajax:old"},
        )
        assert create.status_code == 200
        aid = create.json()["account_id"]

        resp = client.post(
            "/accounts/refresh",
            json={
                "account_id": aid,
                "li_at": "AQEDAWx0Y29va2llYYY",
                "jsessionid": "ajax:new",
                "x_li_track": '{"v":2}',
                "csrf_token": "ajax:csrf99",
            },
        )
        assert resp.status_code == 200
        assert resp.json() == {"ok": True, "account_id": aid}

        from apps.api import main as api_mod

        auth = api_mod.storage.get_account_auth(aid)
        assert auth.li_at == "AQEDAWx0Y29va2llYYY"
        assert auth.jsessionid == "ajax:new"
        assert auth.x_li_track == '{"v":2}'
        assert auth.csrf_token == "ajax:csrf99"

    def test_refresh_merges_omitted_header_fields(self, client):
        create = client.post(
            "/accounts",
            json={
                "label": "r2",
                "li_at": "AQEDAWx0Y29va2llXXX",
                "x_li_track": '{"keep":true}',
                "csrf_token": "ajax:keepcsrf",
            },
        )
        aid = create.json()["account_id"]

        resp = client.post(
            "/accounts/refresh",
            json={"account_id": aid, "li_at": "AQEDAWx0Y29va2llZZZ"},
        )
        assert resp.status_code == 200

        from apps.api import main as api_mod

        auth = api_mod.storage.get_account_auth(aid)
        assert auth.li_at == "AQEDAWx0Y29va2llZZZ"
        assert auth.x_li_track == '{"keep":true}'
        assert auth.csrf_token == "ajax:keepcsrf"

    def test_refresh_unknown_account_404(self, client):
        resp = client.post(
            "/accounts/refresh",
            json={"account_id": 999999, "li_at": "AQEDAWx0Y29va2llXXX"},
        )
        assert resp.status_code == 404


class TestAuthIdentity:
    def test_auth_identity_ok(self, monkeypatch, client):
        from libs.providers.linkedin.provider import LinkedInIdentity

        create = client.post(
            "/accounts",
            json={"label": "t", "li_at": "AQEDAWx0Y29va2llXXX"},
        )
        aid = create.json()["account_id"]

        def fake_identity(self):
            return LinkedInIdentity(public_identifier="jane", member_id="424242")

        monkeypatch.setattr(
            "apps.api.main.LinkedInProvider.fetch_identity",
            fake_identity,
        )

        resp = client.get(f"/auth/identity?account_id={aid}")
        assert resp.status_code == 200
        j = resp.json()
        assert j["status"] == "ok"
        assert j["public_identifier"] == "jane"
        assert j["member_id"] == "424242"

    def test_auth_identity_unknown_account(self, client):
        resp = client.get("/auth/identity?account_id=999999")
        assert resp.status_code == 200
        j = resp.json()
        assert j["status"] == "failed"
        assert j["error"] == "account not found"


def test_parse_me_json_for_identity():
    from libs.providers.linkedin.provider import _parse_me_json_for_identity

    sample = {
        "miniProfile": {
            "publicIdentifier": "someone",
            "entityUrn": "urn:li:fs_miniProfile:ACoAAA",
            "trackingId": "urn:li:member:777777",
        }
    }
    ident = _parse_me_json_for_identity(sample)
    assert ident.public_identifier == "someone"
    assert ident.member_id == "777777"
