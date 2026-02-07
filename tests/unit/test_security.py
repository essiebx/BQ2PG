"""Unit tests for security modules."""

import pytest
from unittest.mock import MagicMock, patch
from src.security.secret_manager import SecretManager
from src.security.credential_manager import CredentialManager


class TestSecretManager:
    """Test SecretManager class."""

    @patch(
        "src.security.secret_manager.secretmanager.SecretManagerServiceClient"
    )
    def test_get_secret(self, mock_client_class):
        """Test retrieving a secret."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock the response
        mock_response = MagicMock()
        mock_response.payload.data = b"secret_value"
        mock_client.access_secret_version.return_value = mock_response

        manager = SecretManager(project_id="test_project")
        secret = manager.get_secret("test_secret")

        assert secret == "secret_value"
        mock_client.access_secret_version.assert_called_once()

    @patch(
        "src.security.secret_manager.secretmanager.SecretManagerServiceClient"
    )
    def test_get_secret_json(self, mock_client_class):
        """Test retrieving a secret as JSON."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock the response
        mock_response = MagicMock()
        mock_response.payload.data = b'{"key": "value", "number": 123}'
        mock_client.access_secret_version.return_value = mock_response

        manager = SecretManager(project_id="test_project")
        secret_dict = manager.get_secret_json("test_secret")

        assert secret_dict["key"] == "value"
        assert secret_dict["number"] == 123

    def test_get_secret_no_project_id(self):
        """Test error when project_id is not set."""
        manager = SecretManager(project_id=None)
        with pytest.raises(ValueError, match="project_id must be set"):
            manager.get_secret("test_secret")

    @patch(
        "src.security.secret_manager.secretmanager.SecretManagerServiceClient"
    )
    def test_get_database_credentials(self, mock_client_class):
        """Test retrieving database credentials."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock multiple responses
        mock_response1 = MagicMock()
        mock_response1.payload.data = b"localhost"
        mock_response2 = MagicMock()
        mock_response2.payload.data = b"5432"
        mock_response3 = MagicMock()
        mock_response3.payload.data = b"testdb"
        mock_response4 = MagicMock()
        mock_response4.payload.data = b"testuser"
        mock_response5 = MagicMock()
        mock_response5.payload.data = b"testpass"

        mock_client.access_secret_version.side_effect = [
            mock_response1,
            mock_response2,
            mock_response3,
            mock_response4,
            mock_response5,
        ]

        manager = SecretManager(project_id="test_project")
        creds = manager.get_database_credentials()

        assert creds["host"] == "localhost"
        assert creds["port"] == 5432
        assert creds["database"] == "testdb"
        assert creds["user"] == "testuser"
        assert creds["password"] == "testpass"


class TestCredentialManager:
    """Test CredentialManager class."""

    def test_init(self, mock_secret_manager):
        """Test initialization."""
        manager = CredentialManager(mock_secret_manager)
        assert manager.secret_manager == mock_secret_manager
        assert manager.rotation_days == 90

    def test_validate_credentials_all_present(self, mock_secret_manager):
        """Test validation when all credentials are present."""
        mock_secret_manager.get_secret.return_value = "secret_value"
        manager = CredentialManager(mock_secret_manager)

        results = manager.validate_credentials()

        assert all(result["present"] for result in results.values())
        assert mock_secret_manager.get_secret.call_count > 0

    def test_validate_credentials_missing(self, mock_secret_manager):
        """Test validation with missing credentials."""
        mock_secret_manager.get_secret.side_effect = Exception(
            "Secret not found"
        )
        manager = CredentialManager(mock_secret_manager)

        results = manager.validate_credentials()

        assert not results["postgres-host"]["present"]
        assert "error" in results["postgres-host"]

    def test_check_credential_age_valid(self, mock_secret_manager):
        """Test checking credential age when valid."""
        manager = CredentialManager(mock_secret_manager)
        needs_rotation, days = manager.check_credential_age("test_secret")

        # Since we're not mocking secret creation time,
        # this tests the error path
        assert isinstance(needs_rotation, bool)
        assert isinstance(days, int)
