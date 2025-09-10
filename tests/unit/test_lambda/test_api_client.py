from requests import sessions
from lambdas.data_extractor.api_client import APIClient

class DummyResp:
    def __init__(self, status=200, json_data=None):
        self.status_code = status
        self._json = json_data or []
    def json(self, **_): return self._json
    def raise_for_status(self): pass

def test_fetch_data_success(monkeypatch):
    def fake_get(self, url, params=None, timeout=None, **kwargs):
        return DummyResp(200, [{"id": 1}])

    monkeypatch.setattr(sessions.Session, "get", fake_get, raising=True)

    cli = APIClient("https://example.com")
    assert cli.fetch_data({}) == [{"id": 1}]
