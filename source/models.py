"""
Data models for vendor automation pipeline.
Defines canonical data structures using dataclasses.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Optional
from enum import Enum
import json


class RowStatus(str, Enum):
    """Status of a processed row."""
    VALID = "valid"
    FLAGGED = "flagged"
    ERROR = "error"
    PENDING = "pending"


class FileStatus(str, Enum):
    """Status of a processed file."""
    PENDING = "pending"
    PROCESSING = "processing"
    PARTIAL_SUCCESS = "partial_success"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class FileMeta:
    """Metadata for a source file."""
    file_id: str  # SHA256 hash of file content
    filename: str
    path: str
    file_type: str  # csv, xlsx, json, xml, pdf, txt
    size_bytes: int
    created_at: datetime
    processed_at: Optional[datetime] = None
    status: FileStatus = FileStatus.PENDING
    raw_copy_path: Optional[str] = None  # Path where raw file is archived
    error_message: Optional[str] = None
    row_count: int = 0
    valid_rows: int = 0
    flagged_rows: int = 0
    error_rows: int = 0
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        d['created_at'] = self.created_at.isoformat()
        if self.processed_at:
            d['processed_at'] = self.processed_at.isoformat()
        d['status'] = self.status.value
        return d


@dataclass
class Row:
    """A single data row after parsing."""
    row_id: str  # UUID or file_id + line_number
    file_id: str
    line_number: int
    raw_data: dict[str, Any]  # Original parsed data
    normalized_data: dict[str, Any] = field(default_factory=dict)  # After normalization
    canonical_data: dict[str, Any] = field(default_factory=dict)  # After mapping to schema
    status: RowStatus = RowStatus.PENDING
    validation_errors: list[dict] = field(default_factory=list)  # [{"field": "...", "rule": "...", "message": "..."}]
    mapping_confidence: dict[str, float] = field(default_factory=dict)  # {field_name: confidence_score}
    ai_suggestions: dict[str, Any] = field(default_factory=dict)  # For AI-assisted mapping
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        d['status'] = self.status.value
        if self.approved_at:
            d['approved_at'] = self.approved_at.isoformat()
        return d


@dataclass
class ValidationError:
    """Single validation error for a row."""
    field_name: str
    rule_type: str  # 'required', 'type', 'range', 'regex', 'unique', 'semantic'
    message: str
    severity: str = "error"  # 'error' or 'warning'


@dataclass
class BatchResult:
    """Result of a batch processing run."""
    batch_id: str  # Timestamp-based ID
    started_at: datetime
    completed_at: Optional[datetime] = None
    file_results: list[FileMeta] = field(default_factory=list)
    total_rows: int = 0
    valid_rows: int = 0
    flagged_rows: int = 0
    error_rows: int = 0
    status: FileStatus = FileStatus.PROCESSING
    errors: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        d = {
            'batch_id': self.batch_id,
            'started_at': self.started_at.isoformat(),
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'file_results': [f.to_dict() for f in self.file_results],
            'total_rows': self.total_rows,
            'valid_rows': self.valid_rows,
            'flagged_rows': self.flagged_rows,
            'error_rows': self.error_rows,
            'status': self.status.value,
            'errors': self.errors,
        }
        return d


@dataclass
class MappingRule:
    """A single field mapping rule for vendor → canonical schema."""
    vendor_field: str
    canonical_field: str
    rule_type: str  # 'exact', 'regex', 'substring', 'function'
    pattern: Optional[str] = None
    priority: int = 10  # Lower = higher priority
    fallback: bool = False  # If True, only use if deterministic rules fail
    confidence: float = 1.0  # Default confidence if no AI override
    
    
@dataclass
class CanonicalSchema:
    """Definition of the canonical data schema."""
    fields: dict[str, dict[str, Any]]  # {field_name: {type, required, description, ...}}
    version: str = "1.0"
    
    def get_required_fields(self) -> list[str]:
        """Get list of required fields."""
        return [f for f, spec in self.fields.items() if spec.get('required', False)]
    
    def get_field_type(self, field_name: str) -> Optional[str]:
        """Get type of a field."""
        return self.fields.get(field_name, {}).get('type')


@dataclass
class ValidationRuleSet:
    """Collection of validation rules for a vendor."""
    vendor_name: str
    rules: list[dict[str, Any]]  # [{field, rule_type, config}, ...]
    cross_field_rules: list[dict[str, Any]] = field(default_factory=list)  # Rules that check multiple fields
    semantic_rules: list[dict[str, Any]] = field(default_factory=list)

    def __iter__(self):
        """Iterate over all rules (field + cross + semantic)."""
        return iter(self.rules + self.cross_field_rules + self.semantic_rules)


# Utility functions for model conversions
def row_to_csv_dict(row: Row, use_canonical: bool = True) -> dict[str, Any]:
    """Convert Row to dict for CSV export, using canonical or normalized data."""
    data = row.canonical_data if use_canonical else row.normalized_data
    return {
        **data,
        '_row_id': row.row_id,
        '_file_id': row.file_id,
        '_line_number': row.line_number,
        '_status': row.status.value,
    }
