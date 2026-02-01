"""Circuit breaker pattern for fault tolerance."""

import time
import logging
from enum import Enum
from typing import Callable, TypeVar, Any, Optional
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Implements circuit breaker pattern for fault tolerance."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception,
    ):
        """Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit.
            recovery_timeout: Seconds to wait before attempting recovery.
            expected_exception: Exception type to catch.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute function through circuit breaker.

        Args:
            func: Function to execute.
            *args: Positional arguments for function.
            **kwargs: Keyword arguments for function.

        Returns:
            Function result.

        Raises:
            Exception: Original exception or CircuitBreakerOpen.
        """
        self._check_circuit_state()

        if self.state == CircuitState.OPEN:
            raise CircuitBreakerOpen(
                f"Circuit breaker is OPEN. Service unavailable. "
                f"Try again after {self.recovery_timeout} seconds."
            )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise

    def _check_circuit_state(self) -> None:
        """Check if circuit should transition states."""
        if self.state == CircuitState.OPEN:
            if self.last_failure_time and (
                time.time() - self.last_failure_time >= self.recovery_timeout
            ):
                logger.info("Circuit breaker attempting recovery (HALF_OPEN state)")
                self.state = CircuitState.HALF_OPEN
                self.failure_count = 0
                self.success_count = 0

    def _on_success(self) -> None:
        """Handle successful call."""
        self.failure_count = 0

        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= 2:
                logger.info("Circuit breaker recovered (CLOSED state)")
                self.state = CircuitState.CLOSED
                self.success_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == CircuitState.HALF_OPEN:
            logger.error("Recovery failed (OPEN state)")
            self.state = CircuitState.OPEN
        elif self.failure_count >= self.failure_threshold:
            logger.error(f"Failure threshold reached ({self.failure_threshold}). Opening circuit.")
            self.state = CircuitState.OPEN

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to apply circuit breaker.

        Args:
            func: Function to decorate.

        Returns:
            Decorated function.
        """

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return self.call(func, *args, **kwargs)

        return wrapper

    def get_state(self) -> str:
        """Get current circuit state."""
        return self.state.value


class CircuitBreakerOpen(Exception):
    """Raised when circuit breaker is open."""

    pass
