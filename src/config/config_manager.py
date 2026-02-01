"""Configuration manager with environment-based and secret integration."""

import os
import logging
from typing import Any, Dict, Optional
import yaml

from src.security.secret_manager import SecretManager

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages application configuration from YAML files and secrets."""

    def __init__(
        self,
        env: str = "development",
        config_path: str = "config",
        project_id: Optional[str] = None,
    ):
        """Initialize configuration manager.

        Args:
            env: Environment name (development, production, staging).
            config_path: Path to configuration directory.
            project_id: GCP project ID for secrets (optional).
        """
        self.env = env
        self.config_path = config_path
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.config: Dict[str, Any] = {}
        self.secret_manager: Optional[SecretManager] = None

        if self.project_id:
            self.secret_manager = SecretManager(self.project_id)

        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from YAML files."""
        try:
            # Load base config
            base_config_file = os.path.join(self.config_path, "settings.yaml")
            if os.path.exists(base_config_file):
                with open(base_config_file) as f:
                    self.config = yaml.safe_load(f) or {}
                logger.info(f"Loaded base configuration from {base_config_file}")

            # Load environment-specific config
            env_config_file = os.path.join(self.config_path, "environments", f"{self.env}.yaml")
            if os.path.exists(env_config_file):
                with open(env_config_file) as f:
                    env_config = yaml.safe_load(f) or {}
                    self._deep_merge(self.config, env_config)
                    logger.info(f"Loaded {self.env} configuration from {env_config_file}")
            else:
                logger.warning(f"Environment config file not found: {env_config_file}")

            # Load environment variables
            self._load_env_vars()

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def _deep_merge(self, base: Dict, override: Dict) -> None:
        """Deep merge override into base dictionary."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def _load_env_vars(self) -> None:
        """Load environment variables and override config."""
        # Database configuration
        if os.getenv("DB_HOST"):
            if "database" not in self.config:
                self.config["database"] = {}
            self.config["database"]["host"] = os.getenv("DB_HOST")
        if os.getenv("DB_PORT"):
            if "database" not in self.config:
                self.config["database"] = {}
            self.config["database"]["port"] = int(os.getenv("DB_PORT", 5432))
        if os.getenv("DB_NAME"):
            if "database" not in self.config:
                self.config["database"] = {}
            self.config["database"]["name"] = os.getenv("DB_NAME")
        if os.getenv("DB_USER"):
            if "database" not in self.config:
                self.config["database"] = {}
            self.config["database"]["user"] = os.getenv("DB_USER")

        # BigQuery configuration
        if os.getenv("GCP_PROJECT"):
            if "bigquery" not in self.config:
                self.config["bigquery"] = {}
            self.config["bigquery"]["project_id"] = os.getenv("GCP_PROJECT")

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value.

        Args:
            key: Configuration key (supports dot notation: "database.host").
            default: Default value if key not found.

        Returns:
            Configuration value or default.
        """
        keys = key.split(".")
        value = self.config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value

    def get_secret(self, secret_id: str, key: str = "") -> str:
        """Get secret from Secret Manager.

        Args:
            secret_id: Secret identifier.
            key: Optional JSON key within secret.

        Returns:
            Secret value.

        Raises:
            ValueError: If secret manager is not configured.
        """
        if not self.secret_manager:
            raise ValueError("Secret manager not configured. Set GOOGLE_CLOUD_PROJECT.")

        if key:
            secret_json = self.secret_manager.get_secret_json(secret_id)
            return secret_json.get(key)
        else:
            return self.secret_manager.get_secret(secret_id)

    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration.

        Returns:
            Database connection configuration.
        """
        db_config = self.get("database", {})

        # Try to load password from secrets if not in config
        if "password" not in db_config and self.secret_manager:
            try:
                db_config["password"] = self.secret_manager.get_secret("postgres-password")
            except Exception as e:
                logger.warning(f"Could not load password from secrets: {e}")

        return db_config

    def get_bigquery_config(self) -> Dict[str, Any]:
        """Get BigQuery configuration.

        Returns:
            BigQuery configuration.
        """
        bq_config = self.get("bigquery", {})

        # Try to load service account from secrets if not in config
        if "service_account_key" not in bq_config and self.secret_manager:
            try:
                bq_config["service_account_key"] = self.secret_manager.get_secret(
                    "bigquery-service-account-key"
                )
            except Exception as e:
                logger.warning(f"Could not load BigQuery service account from secrets: {e}")

        return bq_config

    def to_dict(self) -> Dict[str, Any]:
        """Get full configuration as dictionary.

        Returns:
            Configuration dictionary.
        """
        return self.config.copy()
