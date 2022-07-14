# tests/test_journal_transporter.py

# All tests should only write files to the test/tmp directory,
# which will be cleaned up automatically at the end of each test.

import pytest
import shutil

from unittest.mock import patch

from pathlib import Path
from typer.testing import CliRunner
from typing import Any

from journal_transporter import __app_name__, __version__, cli, config
from journal_transporter.progress import AbstractProgressReporter

runner = CliRunner()

TMP_PATH = Path("./tests/tmp")


def make_tmp():
    TMP_PATH.mkdir(exist_ok=True)


def clean_up():
    if TMP_PATH.exists(): shutil.rmtree(TMP_PATH)


@pytest.fixture(autouse=True)
def around_each():
    make_tmp()
    yield
    clean_up()


def run(*args):
    result = runner.invoke(cli.app, ["--test", *args])
    return result


# Helpers

def create_fake_server(server_name: str = "test_server", host: str = "https://www.example.com",
                       username: str = "username", password: str = "password", type: str = "http") -> Any:
    return run("define-server", server_name, "-t", type, "-h", host, "-u", username, "-p", password)

# Doubles

    class MockTransferHandler:

        def __init__(self, data_directory: Path, source: str, target: str, progress_reporter: AbstractProgressReporter):
            self.data_directory = data_directory
            self.source = source
            self.target = target
            self.progress_reporter = progress_reporter

        def fetch_indexes(self, paths):
            self.indexed = True
            self.index_paths = paths

        def fetch_data(self, paths):
            self.fetched = self.indexed
            self.fetch_paths = paths

        def push_data(self, paths):
            self.pushed = self.fetched
            self.push_paths = paths


# Tests!

def test_version():
    result = run("--version")
    assert result.exit_code == 0
    assert f"{__app_name__} v{__version__}\n" in result.stdout


def test_verbose():
    result = run('--verbose', '--version')
    assert result.exit_code == 0
    assert "version" in result.stdout


def test_init():
    result = run("init", "--data-directory", TMP_PATH)
    assert result.exit_code == 0
    assert Path(TMP_PATH / "data").exists()


def test_configure():
    run("init")

    subdir = "datatest"

    result = run("-v", "configure", "-d", str(TMP_PATH / subdir),
                 "--default-source", "source", "--default-target", "target", "--keep")
    assert result.exit_code == 0
    assert config.get("data_directory") == str(TMP_PATH / subdir)
    assert config.get("default_source") == "source"
    assert config.get("default_target") == "target"
    assert config.get("keep") == "True"


def test_create_server():
    run("init")

    server_name = "test_server"
    host = "https://www.example.com"
    username = "username"
    password = "password"

    result = create_fake_server(server_name, host, username, password)
    assert result.exit_code == 0

    server_def = config.get_server(server_name)
    assert server_def["host"] == host
    assert server_def["username"] == username
    assert server_def["password"] == password


def test_update_server():
    run("init")

    server_name = "test_server"
    create_fake_server(server_name)
    new_user_name = "new_user"

    result = run("define-server", server_name, "-u", new_user_name)
    assert result.exit_code == 0

    server_def = config.get_server(server_name)
    assert server_def["username"] == new_user_name


def test_delete_server():
    run("init")

    server_name = "test_server"
    create_fake_server(server_name)
    result = run("delete-server", server_name, "-f")

    assert result.exit_code == 0
    assert config.get_server(server_name) is None


def test_get_servers():
    run("init")

    server_name = "test_server"
    create_fake_server(server_name)

    result = run("get-server")
    assert result.exit_code == 0
    assert f"Name: {server_name}" in result.stdout


def test_get_server():
    run("init")

    server_name = "test_server"
    host = "https://hecka-journals.com"
    create_fake_server(server_name, host=host)

    result = run("get-server", server_name)
    assert result.exit_code == 0
    assert f"Host: {host}" in result.stdout


def test_get_config():
    run("init")

    result = run("get-config")

    assert result.exit_code == 0
    assert "data_directory" in result.stdout


def test_transfer_errors():
    run("init")

    result = run("transfer")
    assert "Source server is required" in result.stdout
    assert "Target server is required" in result.stdout

    result = run("transfer", "--index-only")
    assert "Source server is required" in result.stdout

    result = run("transfer", "--fetch-only")
    assert "Source server is required" in result.stdout

    result = run("transfer", "--push-only")
    assert "Target server is required" in result.stdout

    result = run("transfer", "--fetch-only", "--push-only")
    assert "Only one of --index-only, --fetch-only, and --push-only can be set" in result.stdout


@patch("journal_transporter.cli.TransferHandler")
def test_transfer(mock_handler):
    run("init")

    create_fake_server("test_source")
    create_fake_server("test_target")

    run("transfer", "--force", "--source", "test_source", "--fetch-only")
    assert mock_handler.called
