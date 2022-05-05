"""Top-level package for Journal Transporter intermediary application"""
# journal_transporter/__init__.py

from functools import wraps
from asyncio import run
import typer

__app_name__ = "journal_transporter"
__version__ = "0.1.0"

(
    SUCCESS,
    DIR_ERROR,
    FILE_ERROR,
    READ_ERROR,
    WRITE_ERROR,
    JSON_ERROR,
    ID_ERROR
) = range(7)

ERRORS = {
    DIR_ERROR: "config directory error",
    FILE_ERROR: "config file error",
    READ_ERROR: "read error",
    WRITE_ERROR: "write_error",
    ID_ERROR: "id error"
}

# The following permits async functions to be run as synchronous functions that Typer supports
# See https://github.com/tiangolo/typer/issues/88#issuecomment-926302761

# This is a standard decorator that takes arguments
# the same way app.command does but with
# app as the first parameter
def async_command(app, *args, **kwargs):
    def decorator(async_func):

        # Now we make a function that turns the async
        # function into a synchronous function.
        # By wrapping async_func we preserve the
        # meta characteristics typer needs to create
        # a good interface, such as the description and
        # argument type hints
        @wraps(async_func)
        def sync_func(*_args, **_kwargs):
            return run(async_func(*_args, **_kwargs))

        # Now use app.command as normal to register the
        # synchronous function
        app.command(*args, **kwargs)(sync_func)

        # We return the async function unmodifed,
        # so its library functionality is preserved
        return async_func

    return decorator

# as a method injection, app will be replaced as self
# making the syntax exactly the same as it used to be.
# put this all in __init__.py and it will be injected into
# the library project wide
typer.Typer.async_command = async_command
