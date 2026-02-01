# src/governance/lineage.py
"""
Data lineage tracking for audit and compliance.
"""

from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
import json
import uuid

from ..monitoring import StructuredLogger

logger = StructuredLogger("lineage", level="INFO")


@dataclass
class DataSource:
    """Data source definition"""
    source_id: str
    name: str
    type: str  # 'bigquery', 'postgresql', 'file', etc.
    location: str  # dataset/table or file path
    schema: Dict[str, str] = field(default_factory=dict)  # column_name: column_type
    description: str = ""
    owner: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "name": self.name,
            "type": self.type,
            "location": self.location,
            "schema": self.schema,
            "description": self.description,
            "owner": self.owner
        }


@dataclass
class TransformationStep:
    """Transformation step in data pipeline"""
    step_id: str
    name: str
    operation: str  # 'filter', 'aggregate', 'join', 'custom', etc.
    input_sources: List[str]  # source_ids
    output_source: str  # source_id
    code: str = ""
    description: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_id": self.step_id,
            "name": self.name,
            "operation": self.operation,
            "input_sources": self.input_sources,
            "output_source": self.output_source,
            "code": self.code,
            "description": self.description,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class LineageRecord:
    """Complete data lineage for a dataset"""
    record_id: str
    source: DataSource
    transformations: List[TransformationStep] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    last_modified: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "source": self.source.to_dict(),
            "transformations": [t.to_dict() for t in self.transformations],
            "created_at": self.created_at.isoformat(),
            "last_modified": self.last_modified.isoformat()
        }


class LineageTracker:
    """Track data lineage through pipeline"""
    
    def __init__(self):
        self.sources: Dict[str, DataSource] = {}
        self.transformations: Dict[str, TransformationStep] = {}
        self.lineage_records: Dict[str, LineageRecord] = {}
        self.lineage_history: List[Dict[str, Any]] = []
    
    def register_source(
        self,
        name: str,
        source_type: str,
        location: str,
        schema: Dict[str, str] = None,
        description: str = "",
        owner: str = ""
    ) -> str:
        """Register data source"""
        source_id = str(uuid.uuid4())
        source = DataSource(
            source_id=source_id,
            name=name,
            type=source_type,
            location=location,
            schema=schema or {},
            description=description,
            owner=owner
        )
        
        self.sources[source_id] = source
        logger.info(
            f"Data source registered: {name}",
            source_id=source_id,
            type=source_type
        )
        
        return source_id
    
    def add_transformation(
        self,
        name: str,
        operation: str,
        input_sources: List[str],
        output_source: str,
        code: str = "",
        description: str = ""
    ) -> str:
        """Add transformation step"""
        step_id = str(uuid.uuid4())
        step = TransformationStep(
            step_id=step_id,
            name=name,
            operation=operation,
            input_sources=input_sources,
            output_source=output_source,
            code=code,
            description=description
        )
        
        self.transformations[step_id] = step
        logger.info(
            f"Transformation registered: {name}",
            step_id=step_id,
            operation=operation
        )
        
        return step_id
    
    def track_lineage(self, output_source_id: str, input_source_ids: List[str], transformation_ids: List[str]):
        """Track complete lineage for output"""
        record_id = str(uuid.uuid4())
        
        if output_source_id not in self.sources:
            logger.error(f"Unknown output source: {output_source_id}")
            return None
        
        record = LineageRecord(
            record_id=record_id,
            source=self.sources[output_source_id],
            transformations=[self.transformations[tid] for tid in transformation_ids if tid in self.transformations]
        )
        
        self.lineage_records[record_id] = record
        self.lineage_history.append({
            "record_id": record_id,
            "timestamp": datetime.now().isoformat(),
            "output_source": output_source_id,
            "input_sources": input_source_ids,
            "transformation_count": len(transformation_ids)
        })
        
        logger.info(
            f"Lineage tracked",
            record_id=record_id,
            output_source=output_source_id,
            input_count=len(input_source_ids)
        )
        
        return record_id
    
    def get_upstream_lineage(self, source_id: str) -> Dict[str, Any]:
        """Get upstream lineage for source"""
        if source_id not in self.sources:
            return None
        
        upstream: Set[str] = set()
        visited: Set[str] = set()
        
        def traverse(sid: str):
            if sid in visited:
                return
            visited.add(sid)
            
            # Find transformations that produce this source
            for step in self.transformations.values():
                if step.output_source == sid:
                    upstream.update(step.input_sources)
                    for input_id in step.input_sources:
                        traverse(input_id)
        
        traverse(source_id)
        
        return {
            "source_id": source_id,
            "source_name": self.sources[source_id].name,
            "upstream_sources": [self.sources[uid].to_dict() for uid in upstream if uid in self.sources],
            "upstream_count": len(upstream)
        }
    
    def get_downstream_lineage(self, source_id: str) -> Dict[str, Any]:
        """Get downstream lineage for source"""
        if source_id not in self.sources:
            return None
        
        downstream: Set[str] = set()
        visited: Set[str] = set()
        
        def traverse(sid: str):
            if sid in visited:
                return
            visited.add(sid)
            
            # Find transformations that use this source
            for step in self.transformations.values():
                if sid in step.input_sources:
                    downstream.add(step.output_source)
                    traverse(step.output_source)
        
        traverse(source_id)
        
        return {
            "source_id": source_id,
            "source_name": self.sources[source_id].name,
            "downstream_sources": [self.sources[did].to_dict() for did in downstream if did in self.sources],
            "downstream_count": len(downstream)
        }
    
    def get_complete_lineage(self, source_id: str) -> Dict[str, Any]:
        """Get complete upstream and downstream lineage"""
        upstream = self.get_upstream_lineage(source_id)
        downstream = self.get_downstream_lineage(source_id)
        
        return {
            "source_id": source_id,
            "upstream": upstream,
            "downstream": downstream,
            "total_related": upstream["upstream_count"] + downstream["downstream_count"]
        }
    
    def export_lineage_graph(self, format: str = "json") -> str:
        """Export lineage as graph"""
        nodes = []
        edges = []
        
        # Create nodes for sources
        for source_id, source in self.sources.items():
            nodes.append({
                "id": source_id,
                "label": source.name,
                "type": "source",
                "data": source.to_dict()
            })
        
        # Create edges for transformations
        for step_id, step in self.transformations.items():
            # Add transformation node
            nodes.append({
                "id": step_id,
                "label": step.name,
                "type": "transformation",
                "data": step.to_dict()
            })
            
            # Add edges from inputs to transformation
            for input_id in step.input_sources:
                edges.append({
                    "source": input_id,
                    "target": step_id,
                    "type": "input"
                })
            
            # Add edge from transformation to output
            edges.append({
                "source": step_id,
                "target": step.output_source,
                "type": "output"
            })
        
        if format == "json":
            return json.dumps({
                "nodes": nodes,
                "edges": edges,
                "metadata": {
                    "total_sources": len(self.sources),
                    "total_transformations": len(self.transformations)
                }
            }, indent=2)
        
        return ""
    
    def get_lineage_report(self) -> Dict[str, Any]:
        """Generate lineage report"""
        return {
            "timestamp": datetime.now().isoformat(),
            "total_sources": len(self.sources),
            "total_transformations": len(self.transformations),
            "total_lineage_records": len(self.lineage_records),
            "recent_lineage": self.lineage_history[-10:] if self.lineage_history else []
        }
