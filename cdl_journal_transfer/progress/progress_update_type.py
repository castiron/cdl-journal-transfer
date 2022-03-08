"""Types of progress updates"""
# cdl_journal_transfer/progress/progress_update_type.py

from enum import Enum, auto

class ProgressUpdateType(Enum):
    # MAJOR updates create a new progress bar. Typically, there should be one MAJOR update
    # per high-level operation (i.e. index, fetch, push).
    MAJOR = 1

    # MINOR updates create new progress bars when verbose, else update the current bar.
    MINOR = 2

    # DETAIL updates will update progress bar messages on verbose mode and print debug messages.
    # It will not clutter up non-verbose content.
    DETAIL = 3

    # DEBUG updates print lines in debug mode. Debug mode does not display progress bars.
    DEBUG = 4


    def verbose(self) -> bool:
        return self.value > 1


    def debug(self) -> bool:
        return self.value > 3
