# scripts/performance/benchmark_suite.py
"""
Comprehensive performance benchmarking suite for BQ2PG pipeline.
"""

import time
import psutil
import json
from typing import Dict, List, Any
from datetime import datetime
import statistics

from src.monitoring import StructuredLogger

logger = StructuredLogger("benchmark", level="INFO")


class BenchmarkSuite:
    """Run comprehensive performance benchmarks"""
    
    def __init__(self):
        self.results: Dict[str, Dict[str, Any]] = {}
        self.start_time = None
        self.end_time = None
    
    def benchmark_memory(self) -> Dict[str, float]:
        """Benchmark memory usage"""
        logger.info("Running memory benchmark")
        
        import gc
        gc.collect()
        
        process = psutil.Process()
        samples = []
        
        for _ in range(10):
            memory_mb = process.memory_info().rss / (1024 * 1024)
            samples.append(memory_mb)
            time.sleep(0.1)
        
        result = {
            "min_mb": min(samples),
            "max_mb": max(samples),
            "avg_mb": statistics.mean(samples),
            "stdev_mb": statistics.stdev(samples) if len(samples) > 1 else 0
        }
        
        logger.info(f"Memory benchmark: {result}")
        return result
    
    def benchmark_cpu(self) -> Dict[str, float]:
        """Benchmark CPU usage"""
        logger.info("Running CPU benchmark")
        
        cpu_samples = []
        
        for _ in range(10):
            cpu_percent = psutil.cpu_percent(interval=0.5)
            cpu_samples.append(cpu_percent)
        
        result = {
            "min_percent": min(cpu_samples),
            "max_percent": max(cpu_samples),
            "avg_percent": statistics.mean(cpu_samples),
            "cpu_count": psutil.cpu_count()
        }
        
        logger.info(f"CPU benchmark: {result}")
        return result
    
    def benchmark_io(self, size_mb: int = 100) -> Dict[str, float]:
        """Benchmark I/O performance"""
        logger.info(f"Running I/O benchmark ({size_mb}MB)")
        
        import tempfile
        import os
        
        # Write benchmark
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
            data = b"x" * (1024 * 1024)  # 1MB chunk
            
            start = time.time()
            for _ in range(size_mb):
                tmp.write(data)
            write_time = time.time() - start
            write_throughput = size_mb / write_time  # MB/s
        
        # Read benchmark
        start = time.time()
        with open(tmp_path, 'rb') as f:
            while f.read(1024 * 1024):
                pass
        read_time = time.time() - start
        read_throughput = size_mb / read_time  # MB/s
        
        os.unlink(tmp_path)
        
        result = {
            "write_throughput_mbs": write_throughput,
            "write_time_seconds": write_time,
            "read_throughput_mbs": read_throughput,
            "read_time_seconds": read_time
        }
        
        logger.info(f"I/O benchmark: {result}")
        return result
    
    def benchmark_list_operations(self) -> Dict[str, float]:
        """Benchmark list operations"""
        logger.info("Running list operations benchmark")
        
        test_size = 100000
        
        # List creation
        start = time.time()
        test_list = list(range(test_size))
        create_time = time.time() - start
        
        # List append
        start = time.time()
        for i in range(test_size):
            test_list.append(i)
        append_time = time.time() - start
        
        # List iteration
        start = time.time()
        for _ in test_list:
            pass
        iteration_time = time.time() - start
        
        result = {
            "create_time_seconds": create_time,
            "append_time_seconds": append_time,
            "iteration_time_seconds": iteration_time,
            "items_per_second": test_size / append_time
        }
        
        logger.info(f"List operations benchmark: {result}")
        return result
    
    def benchmark_dict_operations(self) -> Dict[str, float]:
        """Benchmark dict operations"""
        logger.info("Running dict operations benchmark")
        
        test_size = 100000
        
        # Dict creation
        start = time.time()
        test_dict = {i: f"value_{i}" for i in range(test_size)}
        create_time = time.time() - start
        
        # Dict lookup
        start = time.time()
        for i in range(test_size):
            _ = test_dict.get(i)
        lookup_time = time.time() - start
        
        # Dict iteration
        start = time.time()
        for _ in test_dict.items():
            pass
        iteration_time = time.time() - start
        
        result = {
            "create_time_seconds": create_time,
            "lookup_time_seconds": lookup_time,
            "lookups_per_second": test_size / lookup_time,
            "iteration_time_seconds": iteration_time
        }
        
        logger.info(f"Dict operations benchmark: {result}")
        return result
    
    def benchmark_json_operations(self) -> Dict[str, float]:
        """Benchmark JSON serialization"""
        logger.info("Running JSON operations benchmark")
        
        test_data = [
            {
                "id": i,
                "name": f"item_{i}",
                "value": i * 1.5,
                "metadata": {"key1": f"value_{i}", "key2": i}
            }
            for i in range(10000)
        ]
        
        # Serialization
        start = time.time()
        json_str = json.dumps(test_data)
        serialize_time = time.time() - start
        
        # Deserialization
        start = time.time()
        parsed = json.loads(json_str)
        deserialize_time = time.time() - start
        
        result = {
            "serialize_time_seconds": serialize_time,
            "deserialize_time_seconds": deserialize_time,
            "json_size_kb": len(json_str) / 1024,
            "items_per_second": 10000 / serialize_time
        }
        
        logger.info(f"JSON operations benchmark: {result}")
        return result
    
    def run_all(self) -> Dict[str, Dict[str, Any]]:
        """Run all benchmarks"""
        logger.info("Starting benchmark suite")
        
        self.start_time = datetime.now()
        
        self.results = {
            "memory": self.benchmark_memory(),
            "cpu": self.benchmark_cpu(),
            "io": self.benchmark_io(),
            "list_ops": self.benchmark_list_operations(),
            "dict_ops": self.benchmark_dict_operations(),
            "json_ops": self.benchmark_json_operations()
        }
        
        self.end_time = datetime.now()
        
        logger.info("Benchmark suite completed")
        return self.results
    
    def save_results(self, output_path: str) -> None:
        """Save benchmark results to file"""
        output = {
            "timestamp": self.start_time.isoformat(),
            "duration_seconds": (self.end_time - self.start_time).total_seconds(),
            "results": self.results
        }
        
        with open(output_path, 'w') as f:
            json.dump(output, f, indent=2)
        
        logger.info(f"Results saved to {output_path}")
    
    def print_summary(self) -> None:
        """Print benchmark summary"""
        print("\n" + "=" * 60)
        print("BENCHMARK SUMMARY")
        print("=" * 60)
        
        for category, metrics in self.results.items():
            print(f"\n{category.upper()}:")
            for metric, value in metrics.items():
                if isinstance(value, float):
                    print(f"  {metric}: {value:.2f}")
                else:
                    print(f"  {metric}: {value}")
        
        print("\n" + "=" * 60)


class ScalabilityTester:
    """Test scalability with varying data sizes"""
    
    def __init__(self):
        self.results: List[Dict[str, Any]] = []
    
    def test_chunk_processing(self, chunk_sizes: List[int]) -> List[Dict[str, Any]]:
        """Test performance with different chunk sizes"""
        logger.info(f"Testing chunk processing: {chunk_sizes}")
        
        for size in chunk_sizes:
            # Simulate chunk processing
            start = time.time()
            data = list(range(size))
            processed = [x * 2 for x in data]
            duration = time.time() - start
            
            result = {
                "chunk_size": size,
                "duration_seconds": duration,
                "throughput": size / duration,
                "items_per_second": size / duration
            }
            
            self.results.append(result)
            logger.info(f"Chunk {size}: {duration:.3f}s ({result['items_per_second']:.0f} items/s)")
        
        return self.results
    
    def analyze_scalability(self) -> Dict[str, Any]:
        """Analyze scalability characteristics"""
        if not self.results:
            return {}
        
        throughputs = [r["throughput"] for r in self.results]
        
        analysis = {
            "linear_scalability": self.check_linear_scalability(),
            "optimal_chunk_size": self.get_optimal_chunk_size(),
            "min_throughput": min(throughputs),
            "max_throughput": max(throughputs),
            "avg_throughput": statistics.mean(throughputs)
        }
        
        return analysis
    
    def check_linear_scalability(self) -> bool:
        """Check if throughput scales linearly"""
        if len(self.results) < 2:
            return False
        
        # Simple check: max throughput should be close to min
        throughputs = [r["throughput"] for r in self.results]
        variance = statistics.stdev(throughputs) / statistics.mean(throughputs)
        
        return variance < 0.2  # Less than 20% variation
    
    def get_optimal_chunk_size(self) -> int:
        """Get chunk size with best throughput"""
        best = max(self.results, key=lambda x: x["throughput"])
        return best["chunk_size"]


if __name__ == "__main__":
    # Run benchmark suite
    suite = BenchmarkSuite()
    results = suite.run_all()
    suite.print_summary()
    suite.save_results("benchmark_results.json")
    
    # Test scalability
    logger.info("Running scalability tests")
    tester = ScalabilityTester()
    chunk_sizes = [1000, 5000, 10000, 50000, 100000]
    tester.test_chunk_processing(chunk_sizes)
    analysis = tester.analyze_scalability()
    logger.info(f"Scalability analysis: {analysis}")
