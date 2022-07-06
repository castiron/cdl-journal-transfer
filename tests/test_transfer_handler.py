# transfer/tests/test_transfer_handler.py

# All tests should only write files to the test/tmp directory,
# which will be cleaned up automatically at the end of each test.

import pytest, shutil, json, requests, inflector

from pathlib import Path

from journal_transporter import database, config
from journal_transporter.transfer.transfer_handler import TransferHandler

from tests.shared import around_each, TMP_PATH

@pytest.fixture(autouse=True)
def setup_data_dir():
    database.prepare()


class MockGetResponse:

    def __init__(self, path):
        self.inflector = inflector.English()
        self.path = path.replace(server()["host"] + "/", "")
        self.is_file = "files/" in self.path

        self.headers = { "content-disposition": "attachment; filename='1.pdf'" if self.is_file else "" }

        extension = "pdf" if self.is_file else "json"
        open_mode = "rb" if self.is_file else "r"
        fixture_path = Path("tests/fixtures") / f"{self.path.rstrip('/')}.{extension}"

        with open(fixture_path, open_mode) as file:
            self.content = file.read()

        self.text = self.content


    def json(self):
        return json.loads(self.content)


    def ok(self):
        return True


class MockPostResponse(MockGetResponse):

    def __init__(self, path, **kwargs):
        self.inflector = inflector.English()
        self.path = path.replace(server()["host"] + "/", "")
        self.data = kwargs.get("json") or kwargs.get("data")
        key = self.data["source_record_key"].split(":")[-1]

        fixture_path = Path("tests/fixtures") / f"{self.path.rstrip('/')}/{key}.json"
        if fixture_path.exists():
            with open(fixture_path, "r") as file:
                self.content = file.read()
        else:
            fixture_path = fixture_path.parents[1] / f"{self.inflector.singularize(fixture_path.parent.name)}.json"
            with open(fixture_path, "r") as file:
                index_content = json.loads(file.read())
                the_one = next(x for x in index_content if x["source_record_key"] == self.data["source_record_key"])
                self.content = json.dumps(the_one)

        self.text = self.content


    def json(self):
        content = json.loads(self.content)
        content["source_record_key"] = f"{self.path.rstrip('/').split('/')[-1]}:1"
        return content


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
    return TransferHandler(TMP_PATH, source = server(), target = server())


def mock_get(path, *args, **kwargs):
    return MockGetResponse(path)


def mock_post(path, *args, **kwargs):
    return MockPostResponse(path, **kwargs)


def assert_target_record_key(structure, path):
    if not structure : return

    for resource, definition in structure.items():
        path = path / resource
        detail_file = path / f"{inflector.English().singularize(resource)}.json"

        if detail_file.exists():
            with open(detail_file) as file:
                content = json.loads(file.read())
                content_list = content if isinstance(content, list) else [content]
                for dict in content_list:
                    assert dict.get("target_record_key")

        assert_target_record_key(definition.get("children"), path)


## Tests!

def test_setup(handler):
    assert Path(TMP_PATH).exists()


def test_index(monkeypatch, handler):
    monkeypatch.setattr(requests, "get", mock_get)

    handler.fetch_indexes([])
    assert (TMP_PATH / "current" / "journals" / "index.json").exists()


def test_fetch(monkeypatch, handler):
    monkeypatch.setattr(requests, "get", mock_get)

    handler.fetch_indexes([])
    handler.fetch_data([])
    assert (TMP_PATH / "current" / "journals" / "index.json").exists()


def test_push(monkeypatch, handler):
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)

    handler.fetch_indexes([])
    handler.fetch_data([])
    handler.push_data([])

    path = TMP_PATH
    assert_target_record_key(TransferHandler.STRUCTURE, path)
