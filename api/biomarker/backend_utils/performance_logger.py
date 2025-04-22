"""
Provides a utility class for logging the performance of different parts of the API request processing.

Was used extensively when the API was essentially a GlyGen port. However, as bottlenecks were identified
and progressively fixed, less need for this class.
"""

import time
from typing import Dict, Optional
from logging import Logger
from pprint import pformat


class PerformanceLogger:
    """A utility class to track and log execution times of specific processes.

    Allows starting and stopping timers for different named processes. For processes
    that run multiple times (e.g., within a loop or batch), timings can be grouped
    under a parent name to calculate averages.

    Attributes
    ----------
    logger: Logger
        The logger instance to output performance logs to.
    timings: Dict[str, Dict[str, float]]
        Stroes elapsed times for batch processes, grouped by parent name.
    one_time_timings: Dict[str, float]
        Stores elapsed times for processes that run only once per logging cycle.
    start_times: Dict[str, float]
        Keeps track of active timers using their unique names. Used to calculate
        elapsed time and ensure timers are stopped correctly.
    """

    def __init__(self, logger: Logger):
        """Constructor.

        Parameters
        ----------
        logger: Logger
            The Flask application logger instance to use for output.
        """
        self.timings: Dict = {}
        self.one_time_timings: Dict = {}
        self.start_times: Dict = {}
        self.logger = logger

    def reset(self):
        """Reset all stored timings and active timers.."""
        self.timings = {}
        self.one_time_timings = {}
        self.start_times = {}

    def start_timer(
        self,
        process_name: str,
        parent_name: Optional[str] = None,
    ):
        """Starts a new timer for a given process.

        Parameters
        ----------
        process_name: str
            The name of the process to start timing.
        parent_name: Optional[str], optional
            The name of the parent/batch process, if this timer belongs to a group.
        """
        timer_name = self._get_timer_name(process_name, parent_name)
        self.start_times[timer_name] = time.time()

    def end_timer(self, process_name: str, parent_name: Optional[str] = None):
        """Ends a timer and records the elapsed time. Logs a warning if the timer
        was not started or was already stopped/cancelled.

        Parameters
        ----------
        process_name: str
            The name of the process whose timer should be stopped.
        parent_name: Optional[str], optional
            The name of the parent/batch process, if applicable.
        """
        timer_name = self._get_timer_name(process_name, parent_name)
        if timer_name not in self.start_times:
            self.logger.warning(f"Timer for {timer_name} was likely cancelled.")
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
        process_name: str
            The name of the process whose timer should be cancelled.
        parent_name: Optional[str], optional
            The name of the parent/batch process, if applicable.
        """
        timer_name = self._get_timer_name(process_name, parent_name)
        if timer_name in self.start_times:
            del self.start_times[timer_name]

    def log_times(self, **kwargs):
        """Logs the recorded performance timings to the configured logger.

        Outputs timings for one-time processes and details + averages for batch processes. Also
        logs any additional key-value arguments provided. Resets the logger state afterwards.

        Parameters
        ----------
        **kwargs : Any
            Arbitrary keyword arguments to include in the log output for context.
        """
        log_str = "\n=======================================\n"
        log_str += "KWARGS:\n"
        for key, value in kwargs.items():
            log_str += f"{key}: {pformat(value)}\n"

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
        """Generates a unique name for a timer.

        Uses `parent::process` format for bathc processes, `process` for others.

        Parameters
        ----------
        process_name: str
            The name of the specific process being timed.
        parent_name: Optional[str], optional
            The name of the parent or batch process, if applicable.

        Returns
        -------
        str
            The unique timer name.
        """
        timer_name = (
            f"{parent_name}::{process_name}"
            if parent_name is not None
            else f"{process_name}"
        )
        return timer_name
