"""Memory optimization utilities."""

import logging
import gc
import psutil
from typing import Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MemoryStats:
    """Memory usage statistics."""

    rss_mb: float  # Resident set size
    vms_mb: float  # Virtual memory size
    percent: float  # Percentage of total system memory


class MemoryOptimizer:
    """Optimizes memory usage during pipeline execution."""

    def __init__(self, memory_threshold_percent: float = 80):
        """Initialize memory optimizer.

        Args:
            memory_threshold_percent: Threshold for memory warnings.
        """
        self.memory_threshold_percent = memory_threshold_percent
        self.process = psutil.Process()

    def get_memory_stats(self) -> MemoryStats:
        """Get current process memory statistics.

        Returns:
            MemoryStats object.
        """
        mem_info = self.process.memory_info()
        mem_percent = self.process.memory_percent()

        return MemoryStats(
            rss_mb=mem_info.rss / 1024 / 1024,
            vms_mb=mem_info.vms / 1024 / 1024,
            percent=mem_percent,
        )

    def check_memory_usage(self) -> bool:
        """Check if memory usage is within acceptable limits.

        Returns:
            True if within limits, False otherwise.
        """
        stats = self.get_memory_stats()

        if stats.percent > self.memory_threshold_percent:
            logger.warning(
                f"Memory usage high: {stats.percent:.1f}% "
                f"({stats.rss_mb:.1f}MB RSS)"
            )
            return False

        return True

    def cleanup(self) -> None:
        """Force garbage collection to free memory."""
        try:
            collected = gc.collect()
            logger.debug(f"Garbage collection: freed {collected} objects")
        except Exception as e:
            logger.error(f"Failed to perform garbage collection: {e}")

    def optimize_iterator(self, items: list, batch_size: int = 1000):
        """Create memory-efficient iterator over items.

        Args:
            items: List of items to iterate over.
            batch_size: Size of each batch to yield.

        Yields:
            Batches of items.
        """
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            yield batch
            self.cleanup()

    def estimate_dataframe_memory(self, dataframe) -> float:
        """Estimate memory usage of a DataFrame.

        Args:
            dataframe: Pandas DataFrame.

        Returns:
            Estimated memory usage in MB.
        """
        try:
            return dataframe.memory_usage(deep=True).sum() / 1024 / 1024
        except Exception as e:
            logger.error(f"Failed to estimate DataFrame memory: {e}")
            return 0

    def get_system_memory_info(self) -> Dict[str, Any]:
        """Get system-wide memory information.

        Returns:
            Dictionary with memory information.
        """
        try:
            virtual_memory = psutil.virtual_memory()
            return {
                "total_mb": virtual_memory.total / 1024 / 1024,
                "available_mb": virtual_memory.available / 1024 / 1024,
                "used_mb": virtual_memory.used / 1024 / 1024,
                "percent": virtual_memory.percent,
                "free_mb": virtual_memory.free / 1024 / 1024,
            }
        except Exception as e:
            logger.error(f"Failed to get system memory info: {e}")
            return {}

    def optimize_chunk_size(
        self,
        total_items: int,
        estimated_item_size_mb: float,
        max_memory_mb: float = 500,
    ) -> int:
        """Calculate optimal chunk/batch size for processing.

        Args:
            total_items: Total number of items to process.
            estimated_item_size_mb: Estimated size of each item in MB.
            max_memory_mb: Maximum memory to use for a batch.

        Returns:
            Recommended batch size.
        """
        if estimated_item_size_mb <= 0:
            return max(100, total_items // 10)

        optimal_size = int(max_memory_mb / estimated_item_size_mb)
        optimal_size = max(1, min(optimal_size, total_items))

        logger.debug(
            f"Optimal batch size: {optimal_size} "
            f"(total: {total_items}, item_size: {estimated_item_size_mb}MB)"
        )

        return optimal_size

    def get_memory_report(self) -> Dict[str, Any]:
        """Get comprehensive memory report.

        Returns:
            Dictionary with memory information.
        """
        process_stats = self.get_memory_stats()
        system_stats = self.get_system_memory_info()

        return {
            "process": {
                "rss_mb": process_stats.rss_mb,
                "vms_mb": process_stats.vms_mb,
                "percent_of_system": process_stats.percent,
            },
            "system": system_stats,
            "healthy": process_stats.percent <= self.memory_threshold_percent,
        }
