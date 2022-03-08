# transfer/tests/test_transfer_handler.py

# All tests should only write files to the test/tmp directory,
# which will be cleaned up automatically at the end of each test.

import pytest, shutil, json, requests

from pathlib import Path

from cdl_journal_transfer import database, config
from cdl_journal_transfer.transfer.transfer_handler import TransferHandler

from tests.shared import around_each, TMP_PATH

@pytest.fixture(autouse=True)
def setup_data_dir():
    database.prepare()

class MockResponse:

    def __init__(self, path):
        self.path = path.replace(server()["host"] + "/", "")

    def json(self):
        with open(Path("tests") / "fixtures" / f"{self.path}.json") as file:
            return json.loads(file.read())


## Helpers

def server():
    return {
        "type": "http",
        "host": "https://example.com",
        "username": "source_user",
        "password": "source_password"
    }


@pytest.fixture
def handler():
    return TransferHandler(TMP_PATH, source = server())


## Tests!

def test_setup(handler):
    assert Path(TMP_PATH).exists()


def test_thing(monkeypatch, handler):

    def mock_get(path, *args, **kwargs):
        return MockResponse(path)

    monkeypatch.setattr(requests, "get", mock_get)

    handler.fetch_data([])
    assert (TMP_PATH / "current" / "journals" / "index.json").exists()
