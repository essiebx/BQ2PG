"""End-to-end integration tests for the pipeline."""

import pytest

from unittest.mock import MagicMock, patch

from src.config.config_manager import ConfigManager
from src.pipeline.checkpoint_manager import CheckpointManager
from src.resilience.dead_letter_queue import DeadLetterQueue


class TestEndToEndPipeline:
    """Test complete pipeline workflows."""

    def test_config_loading(self, temp_dir):
        """Test configuration loading from YAML."""
        # Create test YAML files
        config_dir = temp_dir / "config"
        config_dir.mkdir()
        env_dir = config_dir / "environments"
        env_dir.mkdir()

        # Write base config
        with open(config_dir / "settings.yaml", "w") as f:
            f.write("""
database:
  pool_size: 10
pipeline:
  batch_size: 1000
""")

        # Write environment config
        with open(env_dir / "development.yaml", "w") as f:
            f.write("""
database:
  host: localhost
  port: 5432
  name: dev_db
""")

        manager = ConfigManager(env="development", config_path=str(config_dir))

        assert manager.get("database.host") == "localhost"
        assert manager.get("database.port") == 5432
        assert manager.get("database.pool_size") == 10

    def test_checkpoint_recovery(self, temp_dir):
        """Test checkpoint save and recovery."""
        checkpoint_manager = CheckpointManager(
            checkpoint_dir=str(temp_dir / "checkpoints")
        )

        # Save checkpoint
        checkpoint_data = {
            "last_record_id": 1000,
            "offset": 50000,
            "processed_count": 50000,
        }
        checkpoint_id = checkpoint_manager.save_checkpoint(
            "test_pipeline",
            checkpoint_data,
            metadata={"duration_seconds": 10},
        )

        assert checkpoint_id is not None

        # Load checkpoint
        loaded = checkpoint_manager.load_checkpoint(
            "test_pipeline", checkpoint_id
        )

        assert loaded is not None
        assert loaded["data"]["last_record_id"] == 1000
        assert loaded["data"]["offset"] == 50000

    def test_dlq_workflow(self, temp_dir):
        """Test dead letter queue workflow."""
        dlq = DeadLetterQueue(dlq_dir=str(temp_dir / "dlq"))

        # Add failed records
        record1 = {"id": 1, "name": "Alice", "error_field": "invalid_value"}
        record2 = {"id": 2, "name": "Bob", "error_field": None}

        dlq.enqueue(
            record1,
            "Type validation failed",
            source="transform",
            retry_count=3,
        )
        dlq.enqueue(
            record2,
            "Not null constraint violated",
            source="load",
            retry_count=5,
        )

        # Get stats
        stats = dlq.get_dlq_stats()

        assert stats["total_records"] == 2
        assert stats["by_source"]["transform"] == 1
        assert stats["by_source"]["load"] == 1

        # Get records
        records = dlq.get_records()
        assert len(records) == 2
        assert records[0]["error"] in [
            "Type validation failed",
            "Not null constraint violated",
        ]

    def test_integrated_config_and_secrets(self):
        """Test integrated config and secret manager workflow."""
        with patch("src.config.config_manager.SecretManager") as mock_sm_class:
            mock_sm = MagicMock()
            mock_sm.get_secret.return_value = "secret_password"
            mock_sm_class.return_value = mock_sm

            # Create config manager with secret manager
            manager = ConfigManager(project_id="test_project")

            # Try to get database config with secrets
            _ = manager.get_database_config()

            # Should attempt to load password from secrets
            if manager.secret_manager:
                mock_sm.get_secret.assert_called()

    def test_pipeline_execution_flow(self, temp_dir):
        """Test complete pipeline execution flow."""
        # Setup
        checkpoint_dir = temp_dir / "checkpoints"
        dlq_dir = temp_dir / "dlq"
        checkpoint_dir.mkdir()
        dlq_dir.mkdir()

        checkpoint_manager = CheckpointManager(
            checkpoint_dir=str(checkpoint_dir)
        )
        dlq = DeadLetterQueue(dlq_dir=str(dlq_dir))

        # Simulate pipeline processing
        batch_size = 1000
        total_records = 5000

        for i in range(0, total_records, batch_size):
            # Process batch
            batch_num = i // batch_size
            processed = min(batch_size, total_records - i)

            # Some records fail
            if batch_num == 2:
                dlq.enqueue(
                    {"id": 2001},
                    "Duplicate key error",
                    source="load",
                    retry_count=3,
                )

            # Save checkpoint
            checkpoint_manager.save_checkpoint(
                "test_pipeline",
                {
                    "batch": batch_num,
                    "processed": i + processed,
                    "total": total_records,
                },
                metadata={
                    "batch_size": processed,
                    "duration_seconds": 2.5,
                },
            )

        # Verify final state
        checkpoints = checkpoint_manager.list_checkpoints("test_pipeline")
        assert len(checkpoints) > 0

        dlq_stats = dlq.get_dlq_stats()
        assert dlq_stats["total_records"] == 1

    def test_error_recovery_workflow(self, temp_dir):
        """Test error recovery with retry and DLQ."""
        from src.resilience.retry import RetryPolicy

        dlq = DeadLetterQueue(dlq_dir=str(temp_dir / "dlq"))
        retry_policy = RetryPolicy(max_retries=2, initial_delay=0.01)

        attempt_count = 0

        def process_record(record):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise ValueError("Processing failed")
            return {"status": "success", "record": record}

        record = {"id": 123, "data": "test"}

        try:
            result = retry_policy.retry(process_record, record)
            assert result["status"] == "success"
        except Exception as e:
            dlq.enqueue(record, str(e), source="process", retry_count=2)

        # Verify DLQ is empty (no failure)
        stats = dlq.get_dlq_stats()
        assert stats["total_records"] == 0


@pytest.mark.integration
class TestDatabaseIntegration:
    """Test database integration (requires actual database)."""

    @pytest.mark.skip(reason="Requires test database setup")
    def test_postgres_connection(self):
        """Test PostgreSQL connection."""
        # This would require actual database
        pass

    @pytest.mark.skip(reason="Requires test database setup")
    def test_bigquery_connection(self):
        """Test BigQuery connection."""
        # This would require GCP credentials
        pass
