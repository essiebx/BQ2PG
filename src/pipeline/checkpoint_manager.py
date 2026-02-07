"""Checkpoint management for pipeline recovery."""

import json

import logging
from typing import Any, Dict, Optional
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages pipeline checkpoints for recovery and resumption."""

    def __init__(self, checkpoint_dir: str = "checkpoints"):
        """Initialize checkpoint manager.

        Args:
            checkpoint_dir: Directory to store checkpoints.
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.current_checkpoint: Optional[Dict[str, Any]] = None
        logger.info(
            f"Initialized checkpoint manager with directory: "
            f"{self.checkpoint_dir}"
        )

    def save_checkpoint(
        self,
        pipeline_name: str,
        checkpoint_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Save a checkpoint for the pipeline.

        Args:
            pipeline_name: Name of the pipeline.
            checkpoint_data: Data to checkpoint (last processed record,
                            offset, etc.).
            metadata: Optional metadata (e.g., duration, records processed).

        Returns:
            Checkpoint ID.
        """
        checkpoint_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        checkpoint_info = {
            "id": checkpoint_id,
            "pipeline": pipeline_name,
            "timestamp": datetime.utcnow().isoformat(),
            "data": checkpoint_data,
            "metadata": metadata or {},
        }

        checkpoint_file = self.checkpoint_dir / (
            f"checkpoint_{pipeline_name}_{checkpoint_id}.json"
        )

        try:
            with open(checkpoint_file, "w") as f:
                json.dump(checkpoint_info, f, indent=2, default=str)
            self.current_checkpoint = checkpoint_info
            logger.info(
                f"Saved checkpoint {checkpoint_id} for {pipeline_name}"
            )
            return checkpoint_id
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            raise

    def load_checkpoint(
        self, pipeline_name: str, checkpoint_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Load a checkpoint for the pipeline.

        Args:
            pipeline_name: Name of the pipeline.
            checkpoint_id: Specific checkpoint ID (latest if not specified).

        Returns:
            Checkpoint data or None if no checkpoint found.
        """
        try:
            if checkpoint_id:
                checkpoint_file = self.checkpoint_dir / (
                    f"checkpoint_{pipeline_name}_{checkpoint_id}.json"
                )
            else:
                # Find the latest checkpoint
                checkpoints = sorted(
                    self.checkpoint_dir.glob(
                        f"checkpoint_{pipeline_name}_*.json"
                    ),
                    reverse=True,
                )
                if not checkpoints:
                    logger.info(f"No checkpoint found for {pipeline_name}")
                    return None
                checkpoint_file = checkpoints[0]

            if not checkpoint_file.exists():
                logger.warning(f"Checkpoint file not found: {checkpoint_file}")
                return None

            with open(checkpoint_file) as f:
                checkpoint_info = json.load(f)
            self.current_checkpoint = checkpoint_info
            logger.info(
                f"Loaded checkpoint {checkpoint_info['id']} "
                f"for {pipeline_name}"
            )
            return checkpoint_info
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None

    def get_recovery_point(
        self, pipeline_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get the recovery point (last checkpoint data) for a pipeline.

        Args:
            pipeline_name: Name of the pipeline.

        Returns:
            Checkpoint data or None.
        """
        checkpoint = self.load_checkpoint(pipeline_name)
        if checkpoint:
            return checkpoint.get("data")
        return None

    def delete_checkpoint(
        self, pipeline_name: str, checkpoint_id: Optional[str] = None
    ) -> bool:
        """Delete a checkpoint.

        Args:
            pipeline_name: Name of the pipeline.
            checkpoint_id: Specific checkpoint ID (all if not specified).

        Returns:
            True if successful.
        """
        try:
            if checkpoint_id:
                checkpoint_file = self.checkpoint_dir / (
                    f"checkpoint_{pipeline_name}_{checkpoint_id}.json"
                )
                if checkpoint_file.exists():
                    checkpoint_file.unlink()
                    logger.info(f"Deleted checkpoint {checkpoint_id}")
                    return True
            else:
                # Delete all checkpoints for pipeline
                for checkpoint_file in self.checkpoint_dir.glob(
                    f"checkpoint_{pipeline_name}_*.json"
                ):
                    checkpoint_file.unlink()
                    logger.info(f"Deleted checkpoint {checkpoint_file.name}")
                return True
        except Exception as e:
            logger.error(f"Failed to delete checkpoint: {e}")
            return False

    def list_checkpoints(self, pipeline_name: str) -> list:
        """List all checkpoints for a pipeline.

        Args:
            pipeline_name: Name of the pipeline.

        Returns:
            List of checkpoint information dictionaries.
        """
        checkpoints = []
        try:
            for checkpoint_file in sorted(
                self.checkpoint_dir.glob(f"checkpoint_{pipeline_name}_*.json"),
                reverse=True,
            ):
                with open(checkpoint_file) as f:
                    checkpoint_info = json.load(f)
                    checkpoints.append({
                        "id": checkpoint_info["id"],
                        "timestamp": checkpoint_info["timestamp"],
                        "file": checkpoint_file.name,
                    })
        except Exception as e:
            logger.error(f"Failed to list checkpoints: {e}")

        return checkpoints

    def get_checkpoint_stats(self, pipeline_name: str) -> Dict[str, Any]:
        """Get statistics about checkpoints.

        Args:
            pipeline_name: Name of the pipeline.

        Returns:
            Statistics dictionary.
        """
        checkpoints = list(
            self.checkpoint_dir.glob(f"checkpoint_{pipeline_name}_*.json")
        )
        total_size = sum(f.stat().st_size for f in checkpoints)

        return {
            "pipeline": pipeline_name,
            "total_checkpoints": len(checkpoints),
            "total_size_bytes": total_size,
            "oldest": checkpoints[-1].stat().st_mtime if checkpoints else None,
            "newest": checkpoints[0].stat().st_mtime if checkpoints else None,
        }

    def cleanup_old_checkpoints(
        self, pipeline_name: str, keep_count: int = 5
    ) -> int:
        """Clean up old checkpoints, keeping only the most recent ones.

        Args:
            pipeline_name: Name of the pipeline.
            keep_count: Number of recent checkpoints to keep.

        Returns:
            Number of checkpoints deleted.
        """
        try:
            checkpoints = sorted(
                self.checkpoint_dir.glob(f"checkpoint_{pipeline_name}_*.json"),
                reverse=True,
            )

            deleted = 0
            for checkpoint_file in checkpoints[keep_count:]:
                checkpoint_file.unlink()
                deleted += 1
                logger.info(f"Deleted old checkpoint {checkpoint_file.name}")

            return deleted
        except Exception as e:
            logger.error(f"Failed to cleanup old checkpoints: {e}")
            return 0
