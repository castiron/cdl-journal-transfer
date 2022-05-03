import pytest, shutil

from pathlib import Path

from cdl_journal_transfer import config

TMP_PATH = Path("./tests/tmp")

def make_tmp():
    TMP_PATH.mkdir(exist_ok=True)
    config.CONFIG_FILE_PATH = TMP_PATH / "config.ini"
    config.create(TMP_PATH)


def clean_up():
    if TMP_PATH.exists() : shutil.rmtree(TMP_PATH)


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    """Prevent requests from actually sending any requests"""
    monkeypatch.delattr("requests.sessions.Session.request")


@pytest.fixture(autouse=True)
def around_each():
    make_tmp()
    yield
    clean_up()
