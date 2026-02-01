"""Pytest configuration and fixtures."""

import pytest
import os
import tempfile
import logging
from pathlib import Path
from unittest.mock import MagicMock

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_checkpoint_dir(temp_dir):
    """Create a temporary checkpoint directory."""
    checkpoint_dir = temp_dir / "checkpoints"
    checkpoint_dir.mkdir()
    return checkpoint_dir


@pytest.fixture
def temp_dlq_dir(temp_dir):
    """Create a temporary DLQ directory."""
    dlq_dir = temp_dir / "dlq"
    dlq_dir.mkdir()
    return dlq_dir


@pytest.fixture
def mock_secret_manager():
    """Create a mock secret manager."""
    mock = MagicMock()
    mock.get_secret.return_value = "test_secret_value"
    mock.get_secret_json.return_value = {
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_password",
    }
    return mock


@pytest.fixture
def mock_db_connection():
    """Create a mock database connection."""
    mock = MagicMock()
    mock.cursor.return_value = MagicMock()
    mock.is_closed.return_value = False
    return mock


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    mock = MagicMock()
    mock.query.return_value = MagicMock()
    return mock


@pytest.fixture
def sample_records():
    """Provide sample records for testing."""
    return [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "created_at": "2024-01-01"},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "created_at": "2024-01-02"},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com", "created_at": "2024-01-03"},
    ]


@pytest.fixture
def config_dict():
    """Provide sample configuration."""
    return {
        "database": {
            "host": "localhost",
            "port": 5432,
            "name": "test_db",
            "user": "test_user",
            "pool_size": 5,
        },
        "bigquery": {
            "project_id": "test_project",
            "dataset": "test_dataset",
        },
        "pipeline": {
            "batch_size": 1000,
            "parallel_workers": 2,
        },
        "resilience": {
            "max_retries": 3,
            "initial_retry_delay": 1,
        },
    }


@pytest.fixture(autouse=True)
def cleanup_files():
    """Clean up files after each test."""
    yield
    # Cleanup code would go here if needed


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "performance: mark test as a performance test")
    config.addinivalue_line("markers", "slow: mark test as slow")


@pytest.fixture
def env_vars(monkeypatch):
    """Set environment variables for testing."""
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "test_db")
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("GCP_PROJECT", "test_project")
    return monkeypatch
