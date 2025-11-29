"""
CSV exporter for valid and flagged rows.
Handles field selection, filtering, and formatting.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import csv

from .models import Row, RowStatus
from .exporter import Exporter

logger = logging.getLogger(__name__)


class CSVExporter(Exporter):
    """Export rows to CSV format."""
    
    def export(self, rows: List[Row], filename: str, **options) -> Path:
        """
        Export rows to CSV.
        
        Args:
            rows: List of Row objects
            filename: Output filename
            **options:
                - status: RowStatus to filter (VALID, FLAGGED, or None for all)
                - fields: List of field names to include
                - include_metadata: Include _row_id, _status, etc.
                - include_errors: Include validation errors (for flagged)
                - include_raw: Include raw_data column
                - include_normalized: Include normalized_data column
        
        Returns:
            Path to exported CSV file
        """
        status = options.get('status')
        fields = options.get('fields')
        include_metadata = options.get('include_metadata', False)
        include_errors = options.get('include_errors', False)
        include_raw = options.get('include_raw', False)
        include_normalized = options.get('include_normalized', False)
        
        # Filter rows
        filtered_rows = self._filter_rows(rows, status)
        
        if not filtered_rows:
            logger.warning(f"No rows to export for status {status}")
            return None
        
        output_path = self.output_dir / filename
        
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                # Collect all field names
                all_fields = set()
                for row in filtered_rows:
                    if row.canonical_data:
                        all_fields.update(row.canonical_data.keys())
                
                field_list = fields or sorted(list(all_fields))
                
                # Build header
                header = field_list.copy()
                if include_metadata:
                    header.extend(['_row_id', '_line_number', '_status'])
                if include_errors:
                    header.append('_errors')
                if include_raw:
                    header.append('_raw_data')
                if include_normalized:
                    header.append('_normalized_data')
                
                writer = csv.DictWriter(f, fieldnames=header, restval='', 
                                       extrasaction='ignore')
                writer.writeheader()
                
                # Write rows
                for row in filtered_rows:
                    row_dict = self._extract_fields(row, field_list)
                    
                    if include_metadata:
                        row_dict = self._add_metadata(row_dict, row, include_errors)
                    
                    if include_raw:
                        row_dict['_raw_data'] = str(row.raw_data) if row.raw_data else ''
                    
                    if include_normalized:
                        row_dict['_normalized_data'] = str(row.normalized_data) if row.normalized_data else ''
                    
                    writer.writerow(row_dict)
            
            self._log_export(filename, len(filtered_rows), status)
            return output_path
        
        except Exception as e:
            logger.error(f"Error exporting CSV: {e}")
            raise
