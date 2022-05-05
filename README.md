# Journal Transporter

A Python 3 CLI application to handle the transfer of journal data to and from OJS and Janeway.

## Installation

This module requires Python 3. It comes preinstalled on my modern systems, but if not you can
download it at https://www.python.org/downloads/.

Clone this repo and navigate to it. Install dependencies with `bin/install`.

## Usage

CLI commands can be executed with:

```shell
bin/jt <COMMAND>
```

or

```shell
python -m journal_transporter <COMMAND>
```

Note: If you use the latter syntax and invoke python directly, you'll likely want to activate the virtual environment first with `source ./venv/bin/activate`. `bin/jt` does this for you.

### Options

```
  -v, --verbose         Enable verbose output
  --version             Show the application's version and exit
  --install-completion  Install completion for the current shell.
  --show-completion     Show completion for the current shell, to copy it or
                        customize the installation.
  --help                Show this message and exit.
```

### Commands

```
  configure      Apply configuration options.
  define-server  Defines a server that can be referenced later as a source or destination.
  delete-server  Deletes a server configuration.
  get-config     Displays the current application configuration.
  get-server     Displays a server definition, or all server definitions if no argument is provided.
  init           Initialize the application for use.
  transfer       Initiates a transfer of data from a source server to a target server.
```

All commands are described by invoking `bin/jt --help`. Detail on individual commands can be viewed with `bin/jt <COMMAND> --help`.

### Workflow

Generally speaking, the process of transferring a journal from a source server to a target server involves the following commands:

```shell
jt define-server <source_server_name> --host <source_server_plugin_api_url> --username <auth_user> --password <auth_password>
jt define-server <target_server_name> --host <destination_server_plugin_api_url> --username <auth_user> --password <auth_password>
jt transfer --source <source_server_name> --target <target_server_name> --journals <journal_path>
```

See documentation for the `transfer` command (`jt transfer --help`) for more options.

# Licensing
Dunno

# Contributions
Dunno
