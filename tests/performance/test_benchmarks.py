"""Performance and benchmarking tests."""

import pytest
import time
from src.performance.parallel_processor import ParallelProcessor
from src.performance.memory_optimizer import MemoryOptimizer
from src.performance.connection_pool import ConnectionPool
from unittest.mock import MagicMock, patch


class TestParallelProcessing:
    """Test parallel processing performance."""

    def test_process_batches_sequential_vs_parallel(self):
        """Compare sequential vs parallel processing."""
        processor = ParallelProcessor(max_workers=4)

        def process_batch(batch):
            """Simulate processing that takes time."""
            time.sleep(0.1)
            return sum(batch)

        # Create test batches
        batches = [list(range(100)) for _ in range(4)]

        # Process in parallel
        start = time.time()
        results = list(processor.process_batches(batches, process_batch))
        parallel_time = time.time() - start

        # Verify results
        assert len(results) == 4
        assert all(r.success for r in results)

        # Parallel should be faster than sequential
        # (0.1 * 4 = 0.4 seconds sequential)
        # Parallel should complete in ~0.15 seconds (with overhead)
        assert parallel_time < 0.35  # Some buffer for overhead

    def test_map_reduce(self):
        """Test map-reduce functionality."""
        processor = ParallelProcessor(max_workers=4)

        def map_func(item):
            """Square each item."""
            return item ** 2

        def reduce_func(a, b):
            """Sum items."""
            return a + b

        items = list(range(10))

        result = processor.map_reduce(items, map_func, reduce_func)

        # Sum of squares: 1 + 4 + 9 + 16 + 25 + 36 + 49 + 64 + 81 = 285
        # Plus 0^2 = 285
        assert result == 285

    @pytest.mark.performance
    def test_batch_processing_throughput(self):
        """Test throughput of batch processing."""
        processor = ParallelProcessor(max_workers=8)
        batch_count = 100

        def process_batch(batch):
            """Fast processing."""
            return len(batch)

        batches = [[i for i in range(1000)] for _ in range(batch_count)]

        start = time.time()
        results = list(processor.process_batches(batches, process_batch))
        duration = time.time() - start

        successful = sum(1 for r in results if r.success)
        throughput = successful / duration

        print(f"Throughput: {throughput:.2f} batches/second")
        assert successful == batch_count


class TestMemoryOptimization:
    """Test memory optimization utilities."""

    def test_memory_stats(self):
        """Test memory stats collection."""
        optimizer = MemoryOptimizer()

        stats = optimizer.get_memory_stats()

        assert stats.rss_mb > 0
        assert stats.vms_mb > 0
        assert 0 <= stats.percent <= 100

    def test_memory_check(self):
        """Test memory usage checking."""
        optimizer = MemoryOptimizer(memory_threshold_percent=95)

        # Should be within normal limits
        is_healthy = optimizer.check_memory_usage()
        assert isinstance(is_healthy, bool)

    def test_optimize_chunk_size(self):
        """Test optimal chunk size calculation."""
        optimizer = MemoryOptimizer()

        # Calculate optimal size for 1M items, 0.1MB each
        chunk_size = optimizer.optimize_chunk_size(
            total_items=1000000,
            estimated_item_size_mb=0.1,
            max_memory_mb=500,
        )

        # 500MB / 0.1MB per item = 5000 items per chunk
        assert chunk_size == 5000

    def test_optimize_chunk_size_small_items(self):
        """Test chunk size with small items."""
        optimizer = MemoryOptimizer()

        chunk_size = optimizer.optimize_chunk_size(
            total_items=100,
            estimated_item_size_mb=0.001,
            max_memory_mb=500,
        )

        # Should not exceed total items
        assert chunk_size <= 100

    def test_memory_optimizer_iterator(self):
        """Test memory-efficient iterator."""
        optimizer = MemoryOptimizer()

        items = list(range(10000))
        batches = list(optimizer.optimize_iterator(items, batch_size=1000))

        assert len(batches) == 10
        assert sum(len(b) for b in batches) == 10000

    def test_get_system_memory_info(self):
        """Test system memory information retrieval."""
        optimizer = MemoryOptimizer()

        info = optimizer.get_system_memory_info()

        assert "total_mb" in info
        assert "available_mb" in info
        assert "percent" in info
        assert info["total_mb"] > 0

    @pytest.mark.performance
    def test_memory_cleanup_performance(self):
        """Test garbage collection performance."""
        optimizer = MemoryOptimizer()

        # Create some objects
        large_list = [list(range(1000)) for _ in range(1000)]
        del large_list

        start = time.time()
        optimizer.cleanup()
        duration = time.time() - start

        # Cleanup should be fast (< 100ms)
        assert duration < 0.1


class TestConnectionPool:
    """Test connection pooling."""

    @patch("src.performance.connection_pool.pool.SimpleConnectionPool")
    def test_pool_initialization(self, mock_pool_class):
        """Test connection pool initialization."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        pool = ConnectionPool(
            host="localhost",
            port=5432,
            database="test",
            user="test",
            password="test",
            min_connections=5,
            max_connections=20,
        )

        assert pool.min_connections == 5
        assert pool.max_connections == 20

    @patch("src.performance.connection_pool.pool.SimpleConnectionPool")
    def test_get_connection(self, mock_pool_class):
        """Test getting connection from pool."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool

        pool = ConnectionPool(
            host="localhost",
            port=5432,
            database="test",
            user="test",
            password="test",
        )

        conn = pool.get_connection()
        assert conn == mock_connection

    @patch("src.performance.connection_pool.pool.SimpleConnectionPool")
    def test_return_connection(self, mock_pool_class):
        """Test returning connection to pool."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        mock_connection = MagicMock()

        pool = ConnectionPool(
            host="localhost",
            port=5432,
            database="test",
            user="test",
            password="test",
        )

        pool.return_connection(mock_connection)
        mock_pool.putconn.assert_called_once()

    @patch("src.performance.connection_pool.pool.SimpleConnectionPool")
    def test_pool_status(self, mock_pool_class):
        """Test getting pool status."""
        mock_pool = MagicMock()
        mock_pool.closed = 0
        mock_pool_class.return_value = mock_pool

        pool = ConnectionPool(
            host="localhost",
            port=5432,
            database="test",
            user="test",
            password="test",
            min_connections=5,
            max_connections=20,
        )

        status = pool.get_pool_status()

        assert status["min_connections"] == 5
        assert status["max_connections"] == 20


@pytest.mark.slow
class TestEndToEndPerformance:
    """End-to-end performance tests."""

    @pytest.mark.skip(reason="Long running test")
    def test_large_batch_processing(self):
        """Test processing large batches."""
        processor = ParallelProcessor(max_workers=8)

        # Create 10k batches of 100 items
        def process_batch(batch):
            return len(batch)

        batches = [[i for i in range(100)] for _ in range(100)]

        start = time.time()
        results = list(processor.process_batches(batches, process_batch))
        duration = time.time() - start

        print(f"Processed {len(results)} batches in {duration:.2f}s")
