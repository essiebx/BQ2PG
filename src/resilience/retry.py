"""Retry policy with exponential backoff and jitter."""

import random
import time
import logging
from typing import Callable, TypeVar, Any, Optional
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryPolicy:
    """Implements retry logic with exponential backoff."""

    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1,
        backoff_multiplier: float = 2,
        max_delay: float = 60,
        jitter: bool = True,
        retriable_exceptions: Optional[tuple] = None,
    ):
        """Initialize retry policy.

        Args:
            max_retries: Maximum number of retry attempts.
            initial_delay: Initial delay in seconds before first retry.
            backoff_multiplier: Multiplier for exponential backoff.
            max_delay: Maximum delay between retries.
            jitter: Whether to add random jitter to delays.
            retriable_exceptions: Tuple of exceptions to retry on.
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_multiplier = backoff_multiplier
        self.max_delay = max_delay
        self.jitter = jitter
        self.retriable_exceptions = retriable_exceptions or (Exception,)

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number.

        Args:
            attempt: Attempt number (0-indexed).

        Returns:
            Delay in seconds.
        """
        delay = self.initial_delay * (self.backoff_multiplier ** attempt)
        delay = min(delay, self.max_delay)

        if self.jitter:
            # Add random jitter (±10% of delay)
            jitter_amount = delay * 0.1
            delay = delay + random.uniform(-jitter_amount, jitter_amount)

        return max(0, delay)

    def retry(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute function with retry logic.

        Args:
            func: Function to execute.
            *args: Positional arguments for function.
            **kwargs: Keyword arguments for function.

        Returns:
            Function result.

        Raises:
            Exception: The last exception if all retries fail.
        """
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except self.retriable_exceptions as e:
                last_exception = e

                if attempt < self.max_retries:
                    delay = self.calculate_delay(attempt)
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries + 1} retry attempts failed: {e}")

        raise last_exception

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to apply retry logic.

        Args:
            func: Function to decorate.

        Returns:
            Decorated function.
        """

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return self.retry(func, *args, **kwargs)

        return wrapper
