"""Parallel processing for improved performance."""

import logging
import concurrent.futures
from typing import Callable, List, Any, Dict, Optional, Iterator
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result of a processing task."""

    success: bool
    data: Any
    error: Optional[str] = None
    duration_ms: float = 0


class ParallelProcessor:
    """Processes data in parallel for better performance."""

    def __init__(self, max_workers: int = 4, timeout: int = 300):
        """Initialize parallel processor.

        Args:
            max_workers: Maximum number of worker threads.
            timeout: Task timeout in seconds.
        """
        self.max_workers = max_workers
        self.timeout = timeout
        logger.info(f"Initialized ParallelProcessor with {max_workers} workers")

    def process_batches(
        self,
        batches: List[List[Any]],
        process_func: Callable,
        **kwargs,
    ) -> Iterator[ProcessingResult]:
        """Process batches in parallel.

        Args:
            batches: List of data batches to process.
            process_func: Function to process each batch.
            **kwargs: Additional arguments for process_func.

        Yields:
            ProcessingResult for each batch.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            # Submit all tasks
            for i, batch in enumerate(batches):
                future = executor.submit(process_func, batch, **kwargs)
                futures[future] = i

            # Yield results as they complete
            for future in concurrent.futures.as_completed(futures):
                batch_id = futures[future]
                try:
                    start_time = future._start_time
                    result = future.result(timeout=self.timeout)
                    duration = (future._end_time - start_time) * 1000 if hasattr(future, "_end_time") else 0

                    yield ProcessingResult(
                        success=True,
                        data=result,
                        duration_ms=duration,
                    )
                    logger.debug(f"Batch {batch_id} processed successfully")
                except concurrent.futures.TimeoutError:
                    yield ProcessingResult(
                        success=False,
                        data=None,
                        error=f"Batch {batch_id} timeout after {self.timeout}s",
                    )
                except Exception as e:
                    yield ProcessingResult(
                        success=False,
                        data=None,
                        error=f"Batch {batch_id} failed: {str(e)}",
                    )

    def process_items(
        self,
        items: List[Any],
        process_func: Callable,
        batch_size: int = 100,
        **kwargs,
    ) -> List[ProcessingResult]:
        """Process items in parallel batches.

        Args:
            items: Items to process.
            process_func: Function to process items.
            batch_size: Size of each batch.
            **kwargs: Additional arguments for process_func.

        Returns:
            List of processing results.
        """
        # Create batches
        batches = [
            items[i : i + batch_size]
            for i in range(0, len(items), batch_size)
        ]

        # Process batches
        results = list(self.process_batches(batches, process_func, **kwargs))
        return results

    def map_reduce(
        self,
        items: List[Any],
        map_func: Callable,
        reduce_func: Callable,
        **kwargs,
    ) -> Any:
        """Execute map-reduce style processing.

        Args:
            items: Items to process.
            map_func: Mapping function.
            reduce_func: Reduction function.
            **kwargs: Additional arguments for functions.

        Returns:
            Reduced result.
        """
        # Map phase
        mapped_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(map_func, item, **kwargs) for item in items]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=self.timeout)
                    mapped_results.append(result)
                except Exception as e:
                    logger.error(f"Map function failed: {e}")

        # Reduce phase
        if not mapped_results:
            return None

        result = mapped_results[0]
        for partial in mapped_results[1:]:
            result = reduce_func(result, partial)

        return result

    def get_statistics(self) -> Dict[str, Any]:
        """Get processor statistics.

        Returns:
            Dictionary with statistics.
        """
        return {
            "max_workers": self.max_workers,
            "timeout_seconds": self.timeout,
        }
