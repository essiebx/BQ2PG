"""Configuration management module."""

from src.config.config_manager import ConfigManager

# Create default config instance
config = ConfigManager()

__all__ = ["ConfigManager", "config"]
