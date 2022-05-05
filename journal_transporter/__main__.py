"""Journal Transporter entry point"""
# journal_transporter/__main__.py

from journal_transporter import cli, __app_name__

def main():
    cli.app(prog_name=__app_name__)

if __name__ == "__main__":
    main()
