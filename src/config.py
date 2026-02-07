import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Configuration management"""

    # BigQuery
    GOOGLE_CLOUD_PROJECT = os.getenv(
        'GOOGLE_CLOUD_PROJECT', 'just-landing-398407'
    )
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
        'GOOGLE_APPLICATION_CREDENTIALS', 'credentials/key.json'
    )

    # PostgreSQL
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'patents_db')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres123')

    # Pipeline
    DEFAULT_CHUNK_SIZE = int(os.getenv('DEFAULT_CHUNK_SIZE', '50000'))
    MAX_ROWS_PER_RUN = int(os.getenv('MAX_ROWS_PER_RUN', '1000000'))
    DEFAULT_SAMPLE_SIZE = int(os.getenv('DEFAULT_SAMPLE_SIZE', '10000'))

    @property
    def postgres_connection_string(self):
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    def validate(self):
        """Validate configuration"""
        errors = []

        if not self.GOOGLE_CLOUD_PROJECT:
            errors.append("GOOGLE_CLOUD_PROJECT is required")

        creds_path = Path(self.GOOGLE_APPLICATION_CREDENTIALS)
        if not creds_path.exists():
            errors.append(f"Credentials file not found: {creds_path}")

        if not self.POSTGRES_PASSWORD:
            errors.append("POSTGRES_PASSWORD is required")

