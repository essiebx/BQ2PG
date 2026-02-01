# src/processing/distributed.py
"""
Distributed processing using Celery and Dask.
"""

from typing import List, Dict, Any, Callable
from abc import ABC, abstractmethod
import pickle
from datetime import datetime

from ..monitoring import StructuredLogger

logger = StructuredLogger("distributed", level="INFO")


class DistributedProcessor(ABC):
    """Base class for distributed processors"""
    
    @abstractmethod
    def submit_task(self, func: Callable, *args, **kwargs) -> str:
        """Submit task for distributed processing"""
        pass
    
    @abstractmethod
    def get_result(self, task_id: str) -> Any:
        """Get task result"""
        pass
    
    @abstractmethod
    def submit_batch(self, tasks: List[tuple]) -> List[str]:
        """Submit batch of tasks"""
        pass


class CeleryProcessor(DistributedProcessor):
    """Distributed processing using Celery"""
    
    def __init__(self, broker_url: str = "redis://localhost:6379"):
        try:
            from celery import Celery
            self.celery_app = Celery('bq2pg', broker=broker_url)
            logger.info(f"Celery initialized with broker: {broker_url}")
        except ImportError:
            logger.warning("Celery not installed. Install with: pip install celery")
            self.celery_app = None
    
    def submit_task(self, func: Callable, *args, **kwargs) -> str:
        """Submit task to Celery"""
        if not self.celery_app:
            raise RuntimeError("Celery not configured")
        
        try:
            # Register function as Celery task
            @self.celery_app.task
            def async_task():
                return func(*args, **kwargs)
            
            result = async_task.delay()
            logger.info(f"Task submitted: {result.id}")
            return result.id
            
        except Exception as e:
            logger.error(f"Failed to submit task: {e}")
            raise
    
    def get_result(self, task_id: str, timeout: int = 300) -> Any:
        """Get task result"""
        if not self.celery_app:
            raise RuntimeError("Celery not configured")
        
        try:
            result = self.celery_app.AsyncResult(task_id)
            output = result.get(timeout=timeout)
            logger.info(f"Task result retrieved: {task_id}")
            return output
            
        except Exception as e:
            logger.error(f"Failed to get result: {e}")
            raise
    
    def submit_batch(self, tasks: List[tuple]) -> List[str]:
        """Submit batch of tasks"""
        task_ids = []
        for func, args, kwargs in tasks:
            task_id = self.submit_task(func, *args, **kwargs)
            task_ids.append(task_id)
        return task_ids


class DaskProcessor(DistributedProcessor):
    """Distributed processing using Dask"""
    
    def __init__(self, scheduler: str = "processes", n_workers: int = 4):
        try:
            import dask
            self.dask_client = None
            self.scheduler = scheduler
            self.n_workers = n_workers
            logger.info(f"Dask initialized with scheduler: {scheduler}")
        except ImportError:
            logger.warning("Dask not installed. Install with: pip install dask[distributed]")
            self.dask_client = None
    
    def get_client(self):
        """Get or create Dask client"""
        if self.dask_client is None:
            try:
                from dask.distributed import Client
                self.dask_client = Client(scheduler=self.scheduler, n_workers=self.n_workers)
                logger.info("Dask client created")
            except Exception as e:
                logger.error(f"Failed to create Dask client: {e}")
                raise
        return self.dask_client
    
    def submit_task(self, func: Callable, *args, **kwargs) -> str:
        """Submit task to Dask"""
        client = self.get_client()
        
        try:
            future = client.submit(func, *args, **kwargs)
            logger.info(f"Dask task submitted: {future.key}")
            return future.key
            
        except Exception as e:
            logger.error(f"Failed to submit task: {e}")
            raise
    
    def get_result(self, task_id: str) -> Any:
        """Get task result"""
        client = self.get_client()
        
        try:
            result = client.get_result(task_id)
            logger.info(f"Dask task result retrieved: {task_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get result: {e}")
            raise
    
    def submit_batch(self, tasks: List[tuple]) -> List[str]:
        """Submit batch of tasks"""
        client = self.get_client()
        task_ids = []
        
        try:
            futures = client.map(lambda t: t[0](*t[1], **t[2]), tasks)
            task_ids = [f.key for f in futures]
            logger.info(f"Submitted {len(task_ids)} tasks to Dask")
            return task_ids
            
        except Exception as e:
            logger.error(f"Failed to submit batch: {e}")
            raise


class ParallelETL:
    """Parallel ETL processing"""
    
    def __init__(self, processor: DistributedProcessor):
        self.processor = processor
        self.task_history: Dict[str, Dict[str, Any]] = {}
    
    def process_chunks_parallel(
        self,
        chunk_processor: Callable,
        chunks: List[Any],
        max_workers: int = 4
    ) -> List[Any]:
        """Process chunks in parallel"""
        task_ids = []
        
        logger.info(f"Processing {len(chunks)} chunks in parallel")
        
        # Submit all tasks
        for chunk in chunks:
            task_id = self.processor.submit_task(chunk_processor, chunk)
            task_ids.append(task_id)
        
        # Collect results
        results = []
        for i, task_id in enumerate(task_ids):
            try:
                result = self.processor.get_result(task_id)
                results.append(result)
                self.task_history[task_id] = {
                    "status": "completed",
                    "timestamp": datetime.now().isoformat()
                }
                logger.info(f"Chunk {i} completed")
            except Exception as e:
                logger.error(f"Chunk {i} failed: {e}")
                self.task_history[task_id] = {
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
        
        return results
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Get task status"""
        return self.task_history.get(task_id, {"status": "unknown"})


class AnomalyDetector:
    """ML-based anomaly detection for data quality"""
    
    def __init__(self):
        self.model = None
        self.is_trained = False
    
    def train(self, data: List[Dict[str, float]]) -> None:
        """Train anomaly detector"""
        try:
            from sklearn.ensemble import IsolationForest
            
            # Extract features
            features = [[d.get(f, 0) for f in ["quality_score", "null_count", "duplicate_count"]]
                       for d in data]
            
            self.model = IsolationForest(contamination=0.1, random_state=42)
            self.model.fit(features)
            self.is_trained = True
            
            logger.info("Anomaly detector trained")
            
        except ImportError:
            logger.warning("scikit-learn not installed. Install with: pip install scikit-learn")
        except Exception as e:
            logger.error(f"Failed to train anomaly detector: {e}")
    
    def detect_anomalies(self, data: List[Dict[str, float]]) -> List[int]:
        """Detect anomalies in data"""
        if not self.is_trained or not self.model:
            logger.warning("Model not trained")
            return []
        
        try:
            features = [[d.get(f, 0) for f in ["quality_score", "null_count", "duplicate_count"]]
                       for d in data]
            
            predictions = self.model.predict(features)
            anomaly_indices = [i for i, pred in enumerate(predictions) if pred == -1]
            
            logger.info(f"Detected {len(anomaly_indices)} anomalies")
            return anomaly_indices
            
        except Exception as e:
            logger.error(f"Failed to detect anomalies: {e}")
            return []
