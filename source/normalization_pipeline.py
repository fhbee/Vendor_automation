"""
Normalization and mapping pipeline.
Orchestrates normalizer, mapper, and reconciler.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from .models import Row, RowStatus, BatchResult, CanonicalSchema
from .metadata_store import MetadataStore
from .normalizer import Normalizer
from .mapper import FieldMapper, MappingEngine
from .reconciler import Reconciler
from .utils import generate_batch_id

logger = logging.getLogger(__name__)


class NormalizationPipeline:
    """Orchestrates normalization, mapping, and reconciliation."""
    
    def __init__(self,
                 metadata_store: MetadataStore,
                 canonical_schema: CanonicalSchema,
                 mapping_rules: List,
                 normalizer: Optional[Normalizer] = None):
        self.metadata_store = metadata_store
        self.canonical_schema = canonical_schema
        self.mapping_rules = mapping_rules
        self.normalizer = normalizer or Normalizer()
        self.mapper = FieldMapper(mapping_rules)
    
    def process_row(self, row: Row) -> Row:
        """
        Process single row through pipeline.
        
        Args:
            row: Row with raw_data
        
        Returns:
            Row with normalized_data and canonical_data
        """
        # Step 1: Normalize
        row.normalized_data = self.normalizer.normalize_row(row.raw_data)
        
        # Step 2: Map
        canonical_data, confidence = self.mapper.map_row(row)
        row.canonical_data = canonical_data
        row.mapping_confidence = confidence
        
        return row
    
    def process_batch(self, rows: List[Row]) -> List[Row]:
        """
        Process batch of rows.
        
        Args:
            rows: List of Row objects
        
        Returns:
            Processed rows
        """
        processed = []
        
        # Normalize all rows
        for row in rows:
            row.normalized_data = self.normalizer.normalize_row(row.raw_data)
        
        # Map all rows
        mapper_engine = MappingEngine(self.mapper)
        rows = mapper_engine.map_rows(rows)
        
        # Optional: Mark duplicates (only if key_fields configured)
        # rows = Reconciler.mark_duplicates(rows, ['order_id'])
        
        # Persist
        for row in rows:
            self.metadata_store.insert_row(row)
            processed.append(row)
        
        return processed
    
    def process_rows_from_file(self, file_id: str) -> BatchResult:
        """
        Process all rows from a file.
        
        Args:
            file_id: File ID
        
        Returns:
            BatchResult with statistics
        """
        batch_id = generate_batch_id()
        batch_result = BatchResult(
            batch_id=batch_id,
            started_at=datetime.now()
        )
        
        try:
            # Get rows from metadata store
            rows = self.metadata_store.get_rows_by_file(file_id)
            
            if not rows:
                logger.warning(f"No rows found for file {file_id}")
                return batch_result
            
            logger.info(f"Processing {len(rows)} rows from file {file_id}")
            
            # Process rows
            processed = self.process_batch(rows)
            
            batch_result.total_rows = len(processed)
            batch_result.completed_at = datetime.now()
            
            logger.info(f"Processed {len(processed)} rows")
            
        except Exception as e:
            logger.error(f"Error processing file {file_id}: {e}")
            batch_result.errors.append(str(e))
        
        return batch_result
