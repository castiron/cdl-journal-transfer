"""
Progress reporter for CLI app.

The interface for this reporter is the typer module itself, as that is the only way
to end and create new progress bars.
"""
# cdl_journal_transfer/progress/cli_progress_reporter.py

from cdl_journal_transfer.progress.abstract_progress_reporter import AbstractProgressReporter
from cdl_journal_transfer.progress.progress_update_type import ProgressUpdateType
from cdl_journal_transfer import cli

class CliProgressReporter(AbstractProgressReporter):

    def set_progress(self, new_total_progress: int) -> None:
        # Typer/Click takes progress updates as an amount to be added to current progress,
        # not a total value. Therefore, we have to find find the difference between old and
        # new progress to update the UI.
        self.progress_diff = new_total_progress - self.progress
        super().set_progress(new_total_progress)


    ## Protected

    def _update_interface(self) -> None:
        self.progressbar.label = self.message
        progress_diff = self.progress_diff if hasattr(self, "progress_diff") else 0
        self.progressbar.update(progress_diff)

        # Zero out progress_diff in case this gets called again before set_progress.
        self.progress_diff = 0


    def _new_progress_bar(self, length: int, before_message: str = None, bar_init_message: str = "Initializing...", start: int = 0) -> None:
        # Annoyingly, Click requires that progressbars be used in a with block. That doesn't
        # work for us, so we're going to hack our way around it. This may be brittle with
        # Click version upgrades.

        # Exit existing bar, if it exists
        self._close_progress_bar()
        cli.write_line_break()

        self.set_progress(0)
        self.progress_diff = 0

        if before_message : self._print_message(before_message)

        self.progressbar = self.interface.progressbar(**self.__progressbar_options(length))

        # Simulate a block __enter__
        # See https://github.com/pallets/click/blob/d14ee193d01096113d5de0428b8552bcd5f368e9/src/click/_termui_impl.py#L96
        self.progressbar.entered = True
        self.progressbar.render_progress()

        if bar_init_message : self.progressbar.label = bar_init_message
        if start : self.progressbar.update(start)


    def _close_progress_bar(self) -> None:
        # Simulate a block __exit__
        # See https://github.com/pallets/click/blob/d14ee193d01096113d5de0428b8552bcd5f368e9/src/click/_termui_impl.py#L101
        if hasattr(self, "progressbar") : self.progressbar.render_finish()


    def _handle_debug(self, message: str, update_type: ProgressUpdateType = ProgressUpdateType.DEBUG) -> None:
        if update_type is ProgressUpdateType.MAJOR:
            theme = "highlight"
        elif update_type is ProgressUpdateType.MINOR:
            theme = "attention"
        elif update_type is ProgressUpdateType.DETAIL:
            theme = "normal"
        elif update_type is ProgressUpdateType.DEBUG:
            theme = "info"
            
        if message : self._print_message(f"{self._now()} -- {message}", theme)


    def _print_message(self, message: str, theme: str = None) -> None:
        cli.write(message, theme)


    ## Private

    def __progressbar_options(self, length: int) -> dict:
        """
        Parameters:
            length: int
                The length of the progress bar.

        Returns: dict
            Default kwargs for Click's progressbar implementation to standardize formatting.
        """
        return {
            "length": length,
            "width": 100,
            "fill_char": "\u25A0",
            "bar_template": "    [%(bar)s] %(info)s  - %(label)s",
            "update_min_steps": 0
        }
