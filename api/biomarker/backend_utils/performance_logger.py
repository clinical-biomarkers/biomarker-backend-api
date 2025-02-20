import time
from typing import Dict, Optional
from logging import Logger
from pprint import pformat


class PerformanceLogger:
    """Class to keep track of performance logging metrics.

    Attributes
    ----------
    timings : Dict
        Holds the elapsed time information for batched processes.
    one_time_timings : Dict
        Holds the elapsed time infomration for one time processes.
    start_times : Dict
        Keeps track of the active timers to enforce timer end checks.
    """

    def __init__(self, logger: Logger):
        """Constructor.

        Parameters
        ----------
        logger : Logger
            The logger to dump into.
        """
        self.timings: Dict = {}
        self.one_time_timings: Dict = {}
        self.start_times: Dict = {}
        self.logger = logger

    def reset(self):
        """Resets the instance."""
        self.timings = {}
        self.one_time_timings = {}
        self.start_times = {}

    def start_timer(
        self,
        process_name: str,
        parent_name: Optional[str] = None,
    ):
        """Starts a new timer.

        Parameters
        ----------
        process_name : str
            The process name for the timer.
        parent_name : str or None (default: None)
            The parent process name (for batch processes).
        """
        timer_name = self._get_timer_name(process_name, parent_name)
        self.start_times[timer_name] = time.time()

    def end_timer(self, process_name: str, parent_name: Optional[str] = None):
        """Ends a timer. Will log an error if attempting to stop a timer
        that was never started.

        Parameters
        ----------
        process_name : str
            The process name for the timer.
        parent_name : str or None (default: None)
            The parent process name (for batch processes).
        """
        timer_name = self._get_timer_name(process_name, parent_name)
        if timer_name not in self.start_times:
            self.logger.warning(
                f"Timer for {timer_name} was likely cancelled."
            )
            return

        end_time = time.time()
        elapsed_time = end_time - self.start_times.pop(timer_name)
        if parent_name is not None:
            if parent_name not in self.timings:
                self.timings[parent_name] = {}
            if process_name not in self.timings[parent_name]:
                self.timings[parent_name][process_name] = elapsed_time
        else:
            self.one_time_timings[process_name] = elapsed_time

    def cancel_timer(self, process_name: str, parent_name: Optional[str] = None):
        """Cancels a timer without recording its time.

        Parameters
        ----------
        process_name : str
            The process name for the timer.
        parent_name : str or None (default: None)
            The parent process name (for batch processes).
        """
        timer_name = self._get_timer_name(process_name, parent_name)
        if timer_name in self.start_times:
            del self.start_times[timer_name]

    def log_times(self, **kwargs):
        """Dumps the times (and averages for the batch times) to the log."""
        log_str = "\n=======================================\n"
        log_str += "KWARGS:\n"
        for key, value in kwargs.items():
            log_str += f"\t{key}: {pformat(value)}\n"

        log_str += "ONE TIME PROCESSES:\n"
        for process, time_val in self.one_time_timings.items():
            log_str += f"\t{process}: {time_val:.6f}s\n"

        log_str += "BATCH PROCESSES:\n"
        for parent, processes in self.timings.items():
            log_str += f"\t{parent} Details:\n"
            for process, time_val in processes.items():
                log_str += f"\t\t{process}: {time_val:.6f}\n"

        log_str += "Averages:\n"
        for parent, processes in self.timings.items():
            parent_times = list(processes.values())
            average_time = sum(parent_times) / len(parent_times) if parent_times else -1
            log_str += f"\t{parent} - Avg Time: {average_time:.6f}s\n"

        self.logger.info(log_str)
        self.reset()

    def _get_timer_name(
        self, process_name: str, parent_name: Optional[str] = None
    ) -> str:
        timer_name = (
            f"{parent_name}::{process_name}"
            if parent_name is not None
            else f"{process_name}"
        )
        return timer_name
