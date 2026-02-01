"""Unit tests for resilience modules."""

import pytest
import time
from src.resilience.retry import RetryPolicy
from src.resilience.circuit_breaker import CircuitBreaker, CircuitBreakerOpen


class TestRetryPolicy:
    """Test RetryPolicy class."""

    def test_retry_success_first_attempt(self):
        """Test successful function call on first attempt."""
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            return "success"

        policy = RetryPolicy(max_retries=3)
        result = policy.retry(func)

        assert result == "success"
        assert call_count == 1

    def test_retry_success_after_failures(self):
        """Test successful call after retries."""
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary error")
            return "success"

        policy = RetryPolicy(max_retries=3, initial_delay=0.01)
        result = policy.retry(func)

        assert result == "success"
        assert call_count == 3

    def test_retry_all_attempts_fail(self):
        """Test when all retry attempts fail."""
        def func():
            raise ValueError("Persistent error")

        policy = RetryPolicy(max_retries=2, initial_delay=0.01)

        with pytest.raises(ValueError, match="Persistent error"):
            policy.retry(func)

    def test_retry_decorator(self):
        """Test retry as decorator."""
        call_count = 0

        @RetryPolicy(max_retries=2, initial_delay=0.01)
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Error")
            return "success"

        result = func()

        assert result == "success"
        assert call_count == 2

    def test_calculate_delay_exponential_backoff(self):
        """Test exponential backoff calculation."""
        policy = RetryPolicy(
            initial_delay=1,
            backoff_multiplier=2,
            max_delay=60,
            jitter=False,
        )

        delay0 = policy.calculate_delay(0)
        delay1 = policy.calculate_delay(1)
        delay2 = policy.calculate_delay(2)

        assert delay0 == 1
        assert delay1 == 2
        assert delay2 == 4

    def test_calculate_delay_max_delay_limit(self):
        """Test that delay doesn't exceed max_delay."""
        policy = RetryPolicy(
            initial_delay=1,
            backoff_multiplier=2,
            max_delay=10,
            jitter=False,
        )

        delay4 = policy.calculate_delay(4)
        assert delay4 <= 10

    def test_retry_custom_exceptions(self):
        """Test retry only for specific exceptions."""
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            raise TypeError("Not retriable")

        policy = RetryPolicy(
            max_retries=2,
            retriable_exceptions=(ValueError,),
        )

        with pytest.raises(TypeError):
            policy.retry(func)

        # Should fail on first attempt (not retriable)
        assert call_count == 1


class TestCircuitBreaker:
    """Test CircuitBreaker class."""

    def test_circuit_closed_success(self):
        """Test circuit in closed state with successful calls."""
        def func():
            return "success"

        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
        result = breaker.call(func)

        assert result == "success"
        assert breaker.get_state() == "closed"

    def test_circuit_opens_after_threshold(self):
        """Test circuit opens after failure threshold."""
        def func():
            raise Exception("Error")

        breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=60)

        # First failure
        with pytest.raises(Exception):
            breaker.call(func)

        # Second failure
        with pytest.raises(Exception):
            breaker.call(func)

        # Circuit should be open now
        assert breaker.get_state() == "open"

        with pytest.raises(CircuitBreakerOpen):
            breaker.call(func)

    def test_circuit_half_open_recovery(self):
        """Test circuit transitions to half-open and recovers."""
        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)

        # Cause failure to open circuit
        def failing_func():
            raise Exception("Error")

        with pytest.raises(Exception):
            breaker.call(failing_func)

        assert breaker.get_state() == "open"

        # Wait for recovery timeout
        time.sleep(0.15)

        # Circuit should now be half-open
        def success_func():
            return "success"

        result = breaker.call(success_func)
        assert result == "success"
        assert breaker.get_state() == "half_open"

        # Second success should close circuit
        result = breaker.call(success_func)
        assert result == "success"
        assert breaker.get_state() == "closed"

    def test_circuit_decorator(self):
        """Test circuit breaker as decorator."""
        call_count = 0

        @CircuitBreaker(failure_threshold=1, recovery_timeout=1)
        def func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Error")
            return "success"

        # First call fails
        with pytest.raises(Exception):
            func()

        # Circuit is open, next call raises CircuitBreakerOpen
        with pytest.raises(CircuitBreakerOpen):
            func()
