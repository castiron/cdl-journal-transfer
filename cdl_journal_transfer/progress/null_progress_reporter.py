from cdl_journal_transfer.progress.abstract_progress_reporter import AbstractProgressReporter

class NullProgressReporter(AbstractProgressReporter):

    def clean_up(self) -> None:
        pass

    def _update_interface(self) -> None:
        pass


    def _new_progress_bar(self, length: int, before_message: str = None, bar_init_message: str = None, start: int = 0) -> None:
        pass


    def _print_message(self, message: str) -> None:
        pass
