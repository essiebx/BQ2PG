# Credential rotation and validation.
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class CredentialManager:
    """Manages credential lifecycle and rotation."""

    def __init__(self, secret_manager):
        """
        Initialize credential manager.

        Args:
            secret_manager: SecretManager instance
        """
        self.secret_manager = secret_manager
        self.rotation_days = 90
        self.last_rotation_check = None

    def check_credential_age(self, credential_id: str) -> tuple[bool, int]:
        """
        Check if credential needs rotation.

        Args:
            credential_id: The credential to check

        Returns:
            Tuple of (needs_rotation, days_until_expiry)
        """
        # In production, track this in a database or metadata store
        # For now, this is a placeholder
        try:
            # Get metadata about secret
            name = (
                f"projects/{self.secret_manager.project_id}/secrets/"
                f"{credential_id}"
            )
            secret = self.secret_manager.client.get_secret(
                request={"name": name}
            )

            # Check creation time
            if hasattr(secret, 'create_time'):
                created = secret.create_time
                age_days = (datetime.now() - created).days
                days_until_expiry = self.rotation_days - age_days

                needs_rotation = age_days > self.rotation_days

                if needs_rotation:
                    logger.warning(
                        f"Credential {credential_id} is {age_days} days old. "
                        f"Rotation recommended."
                    )

                return needs_rotation, days_until_expiry
        except Exception as e:
            logger.error(f"Failed to check credential age: {e}")
            return False, -1

        return False, 0

    def validate_credentials(self) -> dict:
        """
        Validate all required credentials are present.

        Returns:
            Dictionary with validation results
        """
        required_secrets = [
            "postgres-host",
            "postgres-port",
            "postgres-database",
            "postgres-user",
            "postgres-password",
            "bigquery-service-account-key"
        ]

        results = {}
        for secret_id in required_secrets:
            try:
                value = self.secret_manager.get_secret(secret_id)
                results[secret_id] = {
                    "present": True,
                    "length": len(value) if value else 0
                }
            except Exception as e:
                results[secret_id] = {
                    "present": False,
                    "error": str(e)
                }
                logger.error(f"Missing or invalid secret: {secret_id}")

        return results
