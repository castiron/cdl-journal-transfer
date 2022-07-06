"""
This module provides a CLI interface for managing and performing journal transfers.

Commands:
    init: Initialize config file and data directory
    configure: Update configuration
    get-config: Output the current configuration
    define-server: Create or update a new server definition
    delete-server: Delete an existing server definition
    get-server: Output a server definition (or all server definitions)
    transfer: Perform a data transfer

Color standards:
    White foreground: standard
    Yellow foreground: attention / prompt
    Blue foreground: syntax highlighting
    Magenta foreground: verbose info
    Green foreground: success
    Red foreground: warning / non-fatal error
    Blue background: header
    Green background: great success
    Red background: fatal error
"""
# journal_transporter/cli.py

from pathlib import Path
from typing import Optional, List, Union
from enum import Enum

import typer, asyncio, os

from journal_transporter import __app_name__, __version__, ERRORS, config, database
from journal_transporter.transfer.transfer_handler import TransferHandler
from journal_transporter.progress.cli_progress_reporter import CliProgressReporter

class ConnectionType(str, Enum):
    ssh = "ssh"
    http = "http"

app = typer.Typer()
state = { "verbose": False, "test": False }


# CLI options

def build_option(required: bool = False, default = None, tag: str = "", shortcut: str = None, help: str = None, **opts):
    # Remove Nones from tags
    args = list(filter(None, [tag, shortcut]))

    return typer.Option(
        ... if required else default,
        *args,
        help=help,
        **opts
    )


def opt_data_directory(required: bool = False, default: Path = Path(typer.get_app_dir(__app_name__))):
    return build_option(
        required,
        default,
        "--data-directory",
        "--d",
        "Path to data directory location"
    )


def opt_source(required: bool = False, default: str = None, help: str = "Name of an already-defined source server to use (see define-server)"):
    return build_option(
        required,
        default,
        "--source",
        "-s",
        help
    )


def opt_target(required: bool = False, default: str = None, help: str = "Name of an already-defined target server to use (see define-server)"):
    return build_option(
        required,
        default,
        "--target",
        "-t",
        help
    )


def opt_keep(default: bool = None):
    return build_option(
        False,
        default,
        "--keep / --discard",
        "-k / -K",
        "Should the dataset from this transfer be kept? This could use a lot of disk space"
    )


def opt_keep_max(default: int = None):
    return build_option(
        False,
        default,
        "--keep-max",
        None,
        "If --keep is true, how many transfers should be kept? Older transfer will be discarded."
    )


# Typer utility methods

def verbose() -> bool:
    """Is the CLI in verbose mode?"""
    return state["verbose"] or config.verbose()


def is_test() -> bool:
    """Is the CLI in test mode?"""
    return state["test"] or os.getenv("PYTHON_ENV") == "test"


def color(type):
    """
    Provides Typer kwargs for color themes.

    Parameters:
        type: str
            The type of message being displayed. This determines the color palette.

    Returns:
        dict: Typer kwargs to display proper colors.
    """
    if type == "attention":
        return { "fg":  typer.colors.YELLOW }
    elif type == "info":
        return { "fg":  typer.colors.MAGENTA }
    elif type == "warning":
        return { "fg":  typer.colors.RED }
    elif type == "success":
        return { "fg":  typer.colors.GREEN }
    elif type == "highlight":
        return { "fg": typer.colors.BRIGHT_BLUE}
    elif type == "header":
        return { "fg": typer.colors.WHITE, "bg": typer.colors.BLUE }
    elif type == "error":
        return { "fg": typer.colors.BLACK, "bg": typer.colors.RED }
    elif type == "great_success":
        return { "fg": typer.colors.BLACK, "bg": typer.colors.GREEN }
    else:
        return { "fg": typer.colors.WHITE }


def write(text: str, theme: str = None, line_break: Union[str, bool] = False, **options) -> None:
    """
    Prints to the terminal with standardized formatting.

    Parameters:
        text: str
            Text to display.
        theme: str
            Theme name to determine color palette.
        line_break: Union[str, bool]
            Accepted values: 'before', 'after', 'both', True.
            'before': Adds a line break before the printed text.
            'after': Adds a line break after the printed text.
            'both', True: Adds line breaks both before and after the printed text.
        options: dict
            Arbitrary options to pass to typer.secho as kwargs.
    """
    if line_break in (True, "before", "both") : write_line_break()
    typer.secho(f"    {text}", **color(theme), **options)
    if line_break in (True, "after", "both") : write_line_break()


def verbose_write(text, line_break=False, theme="info", **options) -> None:
    """
    Prints to the terminal if CLI is in verbose mode with standardized formatting.

    Parameters:
        text: str
            Text to display
        line_break: str | bool
            Accepted values: 'before', 'after', 'both', True.
            'before': Adds a line break before the printed text.
            'after': Adds a line break after the printed text.
            'both', True: Adds line breaks both before and after the printed text.
        theme : str
            Theme name to determine color palette. Default for verbose content is 'info'.
        options: dict
            Arbitrary options to pass to typer.secho as kwargs.
    """
    if verbose() : write(text, theme, line_break, **options)


def write_line_break() -> None:
    """Prints a line break to the terminal."""
    write("")


def confirm(text, theme="attention", abort=True, **options):
    """Prints a confirmation prompt to the terminal with standardized formatting."""
    message = typer.style(f"    {text}", **color(theme))
    typer.confirm(message, abort=abort, **options)


def abort_if_errors(errors):
    """
    Prints a message and raises an Exit command if errors are provided.

    Parameters:
        errors: list
            A list of any error messages to be displayed. If the list is empty, noop.
    """
    error_count = len(errors)
    if error_count > 0:
        write(f"{error_count} {'errors have' if error_count > 1 else 'error has'} occurred:", "error")
        for error in errors:
            write(error, "warning")
        raise typer.Exit(1)


## Commands

@app.command()
def init(
    data_directory: Optional[str] = typer.Option(
        None,
        "--data-directory",
        "-d",
        help="Path to data directory location"
    )
) -> None:
    """
    Initialize the application for use. Must be called first.

    This command should be called before all else.
    It creates the data directory and config file, so is required before
    any other configuration.
    """
    write("Initializing Journal Transporter Command Line App...", "header", "after")
    data_directory = Path(data_directory) if data_directory else config.CONFIG_DIR_PATH
    data_directory.mkdir(exist_ok=True)

    config_init_error = config.create(data_directory)
    if config_init_error:
        write(f'ERROR: Creating config file failed with "{ERRORS[config_init_error]}"', "error")
        raise typer.Exit(1)
    verbose_write("Config file: Success!", "info")

    db_init_error = database.create()
    if db_init_error:
        write(f'ERROR: Creating database failed with "{ERRORS[db_init_error]}"', "error")
        raise typer.Exit(1)
    else:
        verbose_write("Data directory: Success!", "info")
        write("\N{party popper} Application initialized! \N{party popper}", "great_success", True)
        write("You can now configure the application with configure and/or define-server.")
        write("Use flag --help for more info.\n")


@app.command()
def configure(
    data_directory: Optional[str] = typer.Option(
        None,
        "--data-directory",
        "-d",
        help="Path to data directory location"
    ),
    default_source: Optional[str] = typer.Option(
        None,
        "--default-source",
        "--s",
        help="Name of an already-defined source server to use by default (see define-server)"
    ),
    default_target: Optional[str] = typer.Option(
        None,
        "--default-target",
        "--t",
        help="Name of an already-defined target server to use by default (see define-server)"
    ),
    keep: Optional[bool] = opt_keep(),
    keep_max: Optional[int] = opt_keep_max(),
    verbose: Optional[bool] = typer.Option(
        None,
        "--verbose / --succinct",
        "-v / -V",
        help="Verbose output by default"
    )
) -> None:
    """
    Apply configuration options.

    These options will be used as default values for initiate-transfer, unless different options are provided
    """
    verbose_write("Applying options...")
    config.apply_options(**locals())

    if data_directory is not None:
        database.create()

    write("Success!", "success")


@app.command()
def define_server(
    name: str = typer.Argument(
        ...,
        help="Name of the server to create or update"
    ),
    host: str = typer.Option(
        None,
        "--host",
        "-h",
        help="The server's URL or hostname that can be used to access it from this machine"
    ),
    type: Optional[ConnectionType] = typer.Option(
        None,
        "--type",
        "-t",
        help="Method that should be used to connect to the server"
    ),
    username: Optional[str] = typer.Option(
        None,
        "--user",
        "-u",
        help="Username of a user authorized to access the information"
    ),
    password: Optional[str] = typer.Option(
        None,
        "--password",
        "-p",
        help="Password of a user authorized to access the information"
    ),
    port: Optional[int] = typer.Option(
        None,
        "--port",
        help="The port to which the client should connect"
    )
) -> None:
    """
    Defines a server that can be referenced later as a source or destination.

    If NAME already exists in the config (case sensitive), this command
    will update that server rather than creating a new one.
    """
    args = {k: v for k, v in locals().items() if v is not None}
    if type : args["type"] = type.value
    existing = config.get_server(name)

    if existing:
        verbose_write(f"A server named '{name}' already exists. Updating...", "after")
    else:
        verbose_write(f"Creating new server '{name}'...", "after")

    result = config.define_server(**args)
    if ERRORS.get(result, False):
        write(f'ERROR: An error occurred: {ERRORS[result]}', "error")
    else:
        verbose_write(f'Name: {name}')
        verbose_write(f'Type: {type}')
        verbose_write(f'Host: {host}')
        if username : verbose_write(f'User: {username}')
        if password : verbose_write(f'Password: {password}')

        verbose_write(
            f'You can reference this server by passing its name ({name}) as the ' +
            f'--source or --destination options of a relevant command.',
            line_break="before"
        )

        write("Server configuration saved!", "success", "before")


@app.command()
def delete_server(
    name: str = typer.Argument(
        ...,
        help="Name of the server you wish to delete"
    ),
    force: Optional[bool] = typer.Option(
        False,
        "--force",
        "-f",
        help="Perform delete without confirmation prompt"
    )
) -> None:
    """
    Deletes a server configuration.
    """
    cfg = config.get_config()
    if cfg.has_section(name):
        if not force:
            text = typer.style(f"Are you sure you want to delete the server named {name}?", **color("attention"))
            typer.confirm(text, abort=True)

        cfg.remove_section(name)
        config._write_config(cfg)
        write(f"Server '{name}' deleted.", "success")
    else:
        write(f"ERROR: Server with name '{name}' not found", "warning")


@app.command()
def get_server(
    name: str = typer.Argument(
        None,
        help="The name of the server you wish to display. Lists all if not provided."
    )
) -> None:
    """
    Displays a server definition, or all server definitions if no argument is provided.
    """
    get_named = bool(name)

    if get_named:
        verbose_write(f"Fetching server named '{name}'...", line_break="after")
    else:
        verbose_write("Fetching all servers...")

    server_list = list(filter(None, [config.get_server(name)])) if get_named else config.get_servers()

    if len(server_list) > 0:
        for index, server_def in enumerate(server_list):
            if not get_named : write(f'#{index + 1}', line_break="before", bold=True)

            for _i, key in enumerate(server_def):
                value = server_def[key]
                if key == "password":
                    value = "*****" if value else ""

                write(f"{key.capitalize()}: {value}")
    else:
        write(f"ERROR: No server definition{f' found with name {name}' if get_named else 's found'}", "warning")


@app.command()
def get_config() -> None:
    """
    Displays the current application configuration.
    """
    cfg = config.get_config()[config.CONFIG_SECTION]

    for key in cfg:
        value = cfg[key]
        styled_key = typer.style(key, **color("highlight"))
        write(f'{styled_key}: {value}')


@app.async_command()
async def transfer(
    journals: List[str] = typer.Option(
        [],
        "--journals",
        "-j",
        help="Any number of journal key names (also known as 'paths' or 'codes') that are to be transferred"
    ),
    source: Optional[str] = opt_source(default = config.get("default_source")),
    target: Optional[str] = opt_target(default = config.get("default_target")),
    fetch_only: Optional[bool] = typer.Option(
        False,
        "--fetch-only",
        help="If true, only fetch data and do not transfer to target server."
    ),
    push_only: Optional[bool] = typer.Option(
        False,
        "--push-only",
        help="If true, only take currently-stored data and transfer to target server. Do not fetch new data."
    ),
    index_only: Optional[bool] = typer.Option(
        False,
        "--index-only",
        help="If true, only index data and do not fetch or push."
    ),
    data_directory: Optional[str] = opt_data_directory(default = config.get("data_directory")),
    keep: Optional[bool] = opt_keep(default = config.get("keep")),
    debug: Optional[bool] = typer.Option(
        False,
        "--debug",
        help="Enable debug output"
    ),
    force: Optional[bool] = typer.Option(
        False,
        "--force",
        "-f",
        help="Run without prompts"
    )
) -> None:
    """
    Initiates a transfer of data from a source server to a target server.
    Where the sausage gets made.
    The real meal deal.
    Where the deals get sausaged.
    Sausage.
    """
    errors = []
    source_def = config.get_server(source)
    target_def = config.get_server(target)

    if source_def == None and not push_only:
        errors.append(f'Source server {f"{source} is not defined" if source is not None else "is required"}')
    if target_def == None and not (index_only or fetch_only):
        errors.append(f'Target server {f"{target} is not defined" if target is not None else "is required"}')

    if len([x for x in [index_only, fetch_only, push_only] if x]) > 1:
        errors.append("Only one of --index-only, --fetch-only, and --push-only can be set. Note that --fetch-only implicitly includes index.")

    abort_if_errors(errors)

    message = f"You are about to transfer {len(journals) or 'ALL'} journal(s) from {f'server `{source}`' if source else 'local storage'} to {f'server `{target}`' if target else 'local storage'}. Are you sure?"
    if not force : confirm(message)

    transfer_methods = []
    if index_only or fetch_only:
        database.prepare(keep)
        transfer_methods = transfer_methods + ["fetch_indexes"]
    if fetch_only:
        transfer_methods = transfer_methods + ["fetch_data"]
    if push_only:
        transfer_methods = transfer_methods + ["push_only"]

    progress_reporter = CliProgressReporter(typer, init_message="Initializing...", verbose = verbose(), debug = debug)
    handler = TransferHandler(data_directory, source=source_def, target=target_def, progress_reporter=progress_reporter)

    for method_name in transfer_methods:
        method = getattr(handler, method_name)
        method(journals)

    progress_reporter.clean_up()


# Callbacks
def __version_callback(value: bool) -> None:
    if value:
        output = f"{__app_name__} version {__version__}" if verbose() else f"{__app_name__} v{__version__}"
        typer.echo(output)
        raise typer.Exit()


def __verbose_callback(value: bool) -> None:
    if value:
        state["verbose"] = True


def __test_callback(value: bool) -> None:
    """
    If test is true, we'll create all files in a tmp directory, then delete them after
    """
    if value:
        state["test"] = True


# Must come after all callback definitions
@app.callback()
def main(
    ctx: typer.Context,
    verbose: Optional[bool] = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose output",
        show_default=False,
        callback=__verbose_callback,
        is_eager=True
    ),
    test: Optional[bool] = typer.Option(
        False,
        "--test",
        callback=__test_callback,
        hidden=True
    ),
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        help="Show the application's version and exit",
        callback=__version_callback,
        is_eager=True
    )
) -> None:
    """A CLI application for transferring journal content from one server to another."""
    state["context"] = ctx
