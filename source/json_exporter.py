"""
JSON exporter for valid and flagged rows.
Supports both JSON array and JSONL (newline-delimited) formats.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import json

from .models import Row, RowStatus
from .exporter import Exporter

logger = logging.getLogger(__name__)


class JSONExporter(Exporter):
    """Export rows to JSON format."""
    
    def export(self, rows: List[Row], filename: str, **options) -> Path:
        """
        Export rows to JSON.
        
        Args:
            rows: List of Row objects
            filename: Output filename
            **options:
                - status: RowStatus filter (VALID, FLAGGED, None)
                - fields: List of fields to include
                - format: 'array' (default) or 'jsonl' (newline-delimited)
                - include_metadata: Include _row_id, _status
                - include_errors: Include validation errors
                - pretty: Pretty print JSON (array format only)
                - indent: Indentation level (default: 2)
        
        Returns:
            Path to exported JSON file
        """
        status = options.get('status')
        fields = options.get('fields')
        json_format = options.get('format', 'array')
        include_metadata = options.get('include_metadata', False)
        include_errors = options.get('include_errors', True)
        pretty = options.get('pretty', True)
        indent = options.get('indent', 2) if pretty else None
        
        filtered_rows = self._filter_rows(rows, status)
        
        if not filtered_rows:
            logger.warning(f"No rows to export for status {status}")
            return None
        
        output_path = self.output_dir / filename
        
        try:
            if json_format == 'jsonl':
                self._export_jsonl(output_path, filtered_rows, fields, 
                                  include_metadata, include_errors)
            else:
                self._export_array(output_path, filtered_rows, fields, 
                                  include_metadata, include_errors, indent)
            
            self._log_export(filename, len(filtered_rows), status)
            return output_path
        
        except Exception as e:
            logger.error(f"Error exporting JSON: {e}")
            raise
    
    def _export_array(self, output_path: Path, rows: List[Row], 
                     fields: Optional[List[str]], include_metadata: bool,
                     include_errors: bool, indent: Optional[int]):
        """Export as JSON array."""
        data = []
        
        for row in rows:
            row_dict = self._extract_fields(row, fields)
            
            if include_metadata:
                row_dict = self._add_metadata(row_dict, row, include_errors)
            
            data.append(row_dict)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
    
    def _export_jsonl(self, output_path: Path, rows: List[Row], 
                     fields: Optional[List[str]], include_metadata: bool,
                     include_errors: bool):
        """Export as JSONL (newline-delimited JSON)."""
        with open(output_path, 'w', encoding='utf-8') as f:
            for row in rows:
                row_dict = self._extract_fields(row, fields)
                
                if include_metadata:
                    row_dict = self._add_metadata(row_dict, row, include_errors)
                
                f.write(json.dumps(row_dict, ensure_ascii=False) + '\n')
