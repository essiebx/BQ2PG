"""Minimal Secret Manager (No external dependencies)."""

import logging

logger = logging.getLogger(__name__)

class SecretManager:
    """Local Secret Manager proxy. External Google Secret Manager disabled."""

    def __init__(self, project_id: str):
        self.project_id = project_id
        logger.info(f"Initialized LocalSecretManager placeholder for: {project_id}")

    def get_secret(self, secret_id: str, version: str = "latest") -> str:
        """Always fails or returns None - users should use environment variables."""
        logger.warning(f"Attempted to access secret '{secret_id}' but Secret Manager is disabled.")
        return ""

    def get_database_credentials(self) -> dict:
        return {}

    def get_bigquery_credentials(self) -> str:
        return ""
