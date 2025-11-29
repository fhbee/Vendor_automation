"""
Validation pipeline orchestrator.
Coordinates structural, cross-field, and semantic validation.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime

from .models import Row, RowStatus, BatchResult, FileStatus
from .metadata_store import MetadataStore
from .validator import RowValidator, ValidationRule
from .cross_field_validator import CrossFieldValidator, CrossFieldRule
from .semantic_validator import SemanticValidator, SemanticRule
from .utils import generate_batch_id

logger = logging.getLogger(__name__)


class ValidationPipeline:
    """Orchestrates validation across all types."""
    
    def __init__(self,
                 metadata_store: MetadataStore,
                 validation_rules: List[ValidationRule] = None,
                 cross_field_rules: List[CrossFieldRule] = None,
                 semantic_rules: List[SemanticRule] = None):
        """
        Initialize validation pipeline.
        
        Args:
            metadata_store: MetadataStore instance
            validation_rules: List of field-level rules
            cross_field_rules: List of cross-field rules
            semantic_rules: List of semantic rules
        """
        self.metadata_store = metadata_store
        self.row_validator = RowValidator(validation_rules or [])
        self.cross_validator = CrossFieldValidator(cross_field_rules or [])
        self.semantic_validator = SemanticValidator(semantic_rules or [])
    
    def validate_row(self, row: Row) -> Row:
        """
        Validate single row through all validation layers.
        
        Args:
            row: Row with canonical_data
        
        Returns:
            Row with validation_errors populated
        """
        errors = []
        
        canonical_data = row.canonical_data or {}
        
        # Layer 1: Field-level validation (structural)
        field_errors = self.row_validator.validate(canonical_data)
        errors.extend(field_errors)
        
        # Layer 2: Cross-field validation (business rules)
        cross_errors = self.cross_validator.validate(canonical_data)
        errors.extend(cross_errors)
        
        # Layer 3: Semantic validation (domain logic)
        semantic_errors = self.semantic_validator.validate(canonical_data)
        errors.extend(semantic_errors)
        
        # Update row
        row.validation_errors = errors
        row.status = RowStatus.VALID if not errors else RowStatus.FLAGGED
        
        logger.debug(f'Row {row.row_id}: {len(errors)} validation errors')
        return row
    
    def validate_batch(self, rows: List[Row]) -> Tuple[List[Row], List[Row]]:
        """
        Validate batch of rows.
        
        Args:
            rows: List of Row objects
        
        Returns:
            Tuple of (valid_rows, flagged_rows)
        """
        valid = []
        flagged = []
        
        for row in rows:
            validated_row = self.validate_row(row)
            self.metadata_store.insert_row(validated_row)
            
            if validated_row.status == RowStatus.VALID:
                valid.append(validated_row)
            else:
                flagged.append(validated_row)
        
        logger.info(f'Batch validation: {len(valid)} valid, {len(flagged)} flagged')
        return valid, flagged
    
    def validate_file(self, file_id: str) -> BatchResult:
        """
        Validate all rows from file.
        
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
            # Get rows
            rows = self.metadata_store.get_rows_by_file(file_id)
            
            if not rows:
                logger.warning(f'No rows found for file {file_id}')
                return batch_result
            
            logger.info(f'Validating {len(rows)} rows from file {file_id}')
            
            # Validate all
            valid, flagged = self.validate_batch(rows)
            
            batch_result.total_rows = len(rows)
            batch_result.valid_rows = len(valid)
            batch_result.flagged_rows = len(flagged)
            batch_result.completed_at = datetime.now()
            batch_result.status = FileStatus.SUCCESS
            
            logger.info(f'Validation complete: {len(valid)} valid, {len(flagged)} flagged')
            
        except Exception as e:
            logger.error(f'Error validating file {file_id}: {e}')
            batch_result.errors.append(str(e))
            batch_result.status = FileStatus.FAILED
        
        return batch_result
