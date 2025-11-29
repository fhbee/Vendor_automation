"""
Base exporter interface and common functionality.
Abstract base for all format-specific exporters.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime

from .models import Row, RowStatus

logger = logging.getLogger(__name__)


class Exporter(ABC):
    """Abstract base exporter class."""
    
    def __init__(self, output_dir: Path):
        """
        Initialize exporter.
        
        Args:
            output_dir: Directory for export files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def export(self, rows: List[Row], filename: str, **options) -> Path:
        """
        Export rows to file.
        
        Args:
            rows: List of Row objects
            filename: Output filename
            **options: Format-specific options
        
        Returns:
            Path to exported file
        """
        pass
    
    def _filter_rows(self, rows: List[Row], status: Optional[RowStatus] = None) -> List[Row]:
        """Filter rows by status (VALID/FLAGGED/None for all)."""
        if status is None:
            return rows
        return [r for r in rows if r.status == status]
    
    def _extract_fields(self, row: Row, fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Extract specified fields from row.
        
        Args:
            row: Row object
            fields: Field names to extract (None = all from canonical_data)
        
        Returns:
            Dict of field:value pairs
        """
        data = row.canonical_data or {}
        
        if fields is None:
            return data
        
        return {f: data.get(f) for f in fields if f in data}
    
    def _add_metadata(self, row_dict: Dict[str, Any], row: Row, 
                      include_errors: bool = False) -> Dict[str, Any]:
        """Add row metadata to export."""
        row_dict['_row_id'] = row.row_id
        row_dict['_line_number'] = row.line_number
        row_dict['_status'] = row.status.value
        
        if include_errors and row.validation_errors:
            row_dict['_errors'] = '|'.join([
                f"{e.get('field')}:{e.get('rule')}" 
                for e in row.validation_errors
            ])
        
        return row_dict
    
    def _log_export(self, filename: str, row_count: int, status: Optional[RowStatus] = None):
        """Log export completion."""
        status_str = f" ({status.value})" if status else ""
        logger.info(f"Exported {row_count} rows{status_str} to {filename}")


class ExportStats:
    """Export operation statistics."""
    
    def __init__(self):
        self.total_rows = 0
        self.valid_rows = 0
        self.flagged_rows = 0
        self.export_count = 0
        self.start_time = datetime.now()
    
    def add_export(self, rows: List[Row]):
        """Record export statistics."""
        self.total_rows += len(rows)
        self.valid_rows += len([r for r in rows if r.status == RowStatus.VALID])
        self.flagged_rows += len([r for r in rows if r.status == RowStatus.FLAGGED])
        self.export_count += 1
    
    def get_duration(self) -> float:
        """Get duration in seconds."""
        return (datetime.now() - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict."""
        return {
            'total_rows': self.total_rows,
            'valid_rows': self.valid_rows,
            'flagged_rows': self.flagged_rows,
            'export_count': self.export_count,
            'duration_seconds': self.get_duration()
        }
