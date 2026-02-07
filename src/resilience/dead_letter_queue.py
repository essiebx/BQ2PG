"""Dead Letter Queue for handling failed records."""

import json

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """Manages dead letter queue for failed records."""

    def __init__(self, dlq_dir: str = "dlq"):
        """Initialize dead letter queue.

        Args:
            dlq_dir: Directory to store DLQ files.
        """
        self.dlq_dir = Path(dlq_dir)
        self.dlq_dir.mkdir(exist_ok=True)
        logger.info(f"Initialized DLQ with directory: {self.dlq_dir}")

    def enqueue(
        self,
        record: Dict[str, Any],
        error: str,
        source: str = "unknown",
        retry_count: int = 0,
    ) -> None:
        """Add a failed record to the DLQ.

        Args:
            record: The failed record.
            error: Error message.
            source: Source of the error (e.g., 'extract', 'load').
            retry_count: Number of retry attempts made.
        """
        dlq_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": source,
            "error": error,
            "retry_count": retry_count,
            "record": record,
        }

        # Create DLQ file
        date_str = datetime.utcnow().strftime("%Y%m%d")
        dlq_file = self.dlq_dir / f"dlq_{source}_{date_str}.jsonl"

        try:
            with open(dlq_file, "a") as f:
                f.write(json.dumps(dlq_entry) + "\n")
            logger.warning(f"Record added to DLQ: {dlq_file} - {error}")
        except Exception as e:
            logger.error(f"Failed to write to DLQ: {e}")

    def get_dlq_stats(self) -> Dict[str, Any]:
        """Get statistics about DLQ files.

        Returns:
            Dictionary with DLQ statistics.
        """
        stats = {
            "total_files": 0,
            "total_records": 0,
            "by_source": {},
            "files": [],
        }

        try:
            for dlq_file in self.dlq_dir.glob("dlq_*.jsonl"):
                stats["total_files"] += 1
                record_count = 0

                with open(dlq_file) as f:
                    for _ in f:
                        record_count += 1
                        stats["total_records"] += 1

                # Extract source from filename
                parts = dlq_file.stem.split("_")
                if len(parts) >= 2:
                    source = parts[1]
                    stats["by_source"][source] = (
                        stats["by_source"].get(source, 0) + record_count
                    )

                stats["files"].append({
                    "name": dlq_file.name,
                    "records": record_count,
                    "size_bytes": dlq_file.stat().st_size,
                })

        except Exception as e:
            logger.error(f"Failed to get DLQ stats: {e}")

        return stats

    def get_records(
        self, source: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get records from DLQ.

        Args:
            source: Filter by source (optional).
            limit: Maximum number of records to retrieve.

        Returns:
            List of DLQ records.
        """
        records = []
        count = 0

        try:
            pattern = f"dlq_{source}_*.jsonl" if source else "dlq_*.jsonl"

            for dlq_file in sorted(self.dlq_dir.glob(pattern), reverse=True):
                with open(dlq_file) as f:
                    for line in f:
                        if count >= limit:
                            break
                        try:
                            record = json.loads(line)
                            records.append(record)
                            count += 1
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in DLQ: {dlq_file}")
                if count >= limit:
                    break

        except Exception as e:
            logger.error(f"Failed to read DLQ records: {e}")

        return records

    def replay_records(
        self, callback, source: Optional[str] = None
    ) -> Dict[str, int]:
        """Replay DLQ records through a callback function.

        Args:
            callback: Function to call with each record.
            source: Filter by source (optional).

        Returns:
            Dictionary with replay statistics.
        """
        stats = {"total": 0, "succeeded": 0, "failed": 0}

        try:
            pattern = f"dlq_{source}_*.jsonl" if source else "dlq_*.jsonl"

            for dlq_file in self.dlq_dir.glob(pattern):
                with open(dlq_file) as f:
                    for line in f:
                        stats["total"] += 1
                        try:
                            record = json.loads(line)
                            callback(record)
                            stats["succeeded"] += 1
                        except Exception as e:
                            logger.error(f"Failed to replay record: {e}")
                            stats["failed"] += 1

        except Exception as e:
            logger.error(f"Failed to replay DLQ records: {e}")

        return stats

    def clear_dlq(
        self, source: Optional[str] = None, older_than_days: int = 0
    ) -> int:
        """Clear DLQ files.

        Args:
            source: Clear only specific source (optional).
            older_than_days: Only clear files older than N days (0 = all).

        Returns:
            Number of files deleted.
        """
        deleted = 0

        try:
            pattern = f"dlq_{source}_*.jsonl" if source else "dlq_*.jsonl"
            cutoff_time = datetime.utcnow().timestamp() - (
                older_than_days * 86400
            )

            for dlq_file in self.dlq_dir.glob(pattern):
                file_time = dlq_file.stat().st_mtime
                if older_than_days == 0 or file_time < cutoff_time:
                    dlq_file.unlink()
                    deleted += 1
                    logger.info(f"Deleted DLQ file: {dlq_file}")

        except Exception as e:
            logger.error(f"Failed to clear DLQ: {e}")

        return deleted
