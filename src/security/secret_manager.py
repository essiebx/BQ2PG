#Google Secret Manager integration
from google.cloud import secretmanager
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class SecretManager:
    """Manages secrets using Google Secret Manager."""
    
    def __init__(self, project_id: str):
        """
        Initialize Secret Manager client.
        
        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()
        logger.info(f"Initialized SecretManager for project: {project_id}")
    
    def get_secret(self, secret_id: str, version: str = "latest") -> str:
        """
        Retrieve a secret from Google Secret Manager.
        
        Args:
            secret_id: The ID of the secret
            version: Version of the secret (default: 'latest')
            
        Returns:
            The secret value as a string
            
        Raises:
            Exception: If secret retrieval fails
        """
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
            response = self.client.access_secret_version(request={"name": name})
            payload = response.payload.data.decode("UTF-8")
            logger.debug(f"Successfully retrieved secret: {secret_id}")
            return payload
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_id}: {e}")
            raise
    
    def get_database_credentials(self) -> dict:
        """
        Retrieve database credentials from Secret Manager.
        
        Returns:
            Dictionary with database connection parameters
        """
        return {
            "host": self.get_secret("postgres-host"),
            "port": int(self.get_secret("postgres-port")),
            "database": self.get_secret("postgres-database"),
            "user": self.get_secret("postgres-user"),
            "password": self.get_secret("postgres-password")
        }
    
    def get_bigquery_credentials(self) -> str:
        """
        Retrieve BigQuery service account JSON.
        
        Returns:
            Service account JSON as string
        """
        return self.get_secret("bigquery-service-account-key")