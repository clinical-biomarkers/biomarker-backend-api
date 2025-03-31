import time
import threading
from typing import Dict
from collections import deque
import os
from dotenv import load_dotenv


class RateLimiter:

    def __init__(self, max_requests: int = 60, time_window: int = 3600) -> None:
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_timestamps: deque = deque()
        self.lock = threading.Lock()

        load_dotenv()
        env_max_requests = os.getenv("AI_SEARCH_MAX_REQUESTS_PER_HOUR")
        if env_max_requests and env_max_requests.isdigit():
            self.max_requests = int(env_max_requests)

    def can_make_request(self) -> bool:
        """Check if a new request is allowed within the rate limit."""
        current_time = time.time()

        with self.lock:
            # Remove timestamps that are outside the time window
            while (
                self.request_timestamps
                and self.request_timestamps[0] < current_time - self.time_window
            ):
                self.request_timestamps.popleft()

            # Check if we're under the limit
            if len(self.request_timestamps) < self.max_requests:
                self.request_timestamps.append(current_time)
                return True

            return False

    def get_status(self) -> Dict:
        """Get the current status of the rate limiter."""
        current_time = time.time()

        current_time = time.time()

        with self.lock:
            while (
                self.request_timestamps
                and self.request_timestamps[0] < current_time - self.time_window
            ):
                self.request_timestamps.popleft()

            # Calculate remaining requests and time until reset
            requests_used = len(self.request_timestamps)
            requests_remaining = self.max_requests - requests_used

            # If there are timestamps, calculate time until oldest drops off
            if self.request_timestamps:
                oldest_timestamp = self.request_timestamps[0]
                time_until_reset = max(
                    0, oldest_timestamp + self.time_window - current_time
                )
            else:
                time_until_reset = 0

            return {
                "requests_used": requests_used,
                "requests_remaining": requests_remaining,
                "max_requests": self.max_requests,
                "time_window_seconds": self.time_window,
                "time_until_reset": int(time_until_reset),
            }

ai_search_rate_limiter = RateLimiter()
