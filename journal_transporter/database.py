"""This module provides access to and management of the data directory."""
# journal_transporter/database.py

import configparser, shutil, os
from pathlib import Path
from datetime import datetime

import typer

from journal_transporter import WRITE_ERROR, SUCCESS, __app_name__, config


def get_database_path() -> Path:
    """Return the current path to the data directory"""
    return config.get("data_directory")


def create() -> int:
    """Create the data directory"""
    db_path = Path(get_database_path())

    try:
        db_path.mkdir(exist_ok=True)
        (db_path / "data").mkdir(exist_ok=True)

        return SUCCESS
    except OSError:
        return WRITE_ERROR


def prepare(keep=None):
    keep = config.get("keep") if keep is None else keep

    base = Path(get_database_path())
    current = base / "current"
    if current.exists():
        current.unlink() if current.is_symlink() else shutil.rmtree(current)

    if keep:
        real = base / datetime.now().strftime("%Y%m%dT%H%M%S")
        real.mkdir()
        os.symlink(real, current)

        enforce_keep_limit()
    else:
        current.mkdir()


def enforce_keep_limit():
    limit = config.get("keep_limit")
    if not limit : return


    base = Path(get_database_path())
    dir_names = []

    for path in base.iterdir():
        if path.is_dir() and path.name != "current":
            dir_names.append(path.name)

    if len(dir_names) > limit:
        doomed_name = sorted(dir_names)[-1]
        doomed_dir = base / doomed_name
        if doomed_dir.exists() : doomed_dir.unlink()
