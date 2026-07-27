from pathlib import Path
import sys


MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

import auto_curator  # noqa: E402


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_fetch_sports_markets_paginates_at_gamma_row_cap(monkeypatch):
    requested_offsets = []

    def fake_get(_url, *, params, timeout):
        assert timeout == 20
        requested_offsets.append(params["offset"])
        if params["offset"] == 0:
            return _Response([{"id": i} for i in range(100)])
        if params["offset"] == 100:
            return _Response([{"id": i} for i in range(100, 150)])
        raise AssertionError(f"unexpected offset {params['offset']}")

    monkeypatch.setattr(auto_curator.requests, "get", fake_get)
    curator = auto_curator.AutoCurator.__new__(auto_curator.AutoCurator)

    markets = curator._fetch_sports_markets()

    assert len(markets) == 150
    assert requested_offsets == [0, 100]
