"""
Validation report generator.
Produces summary reports of validation results.
"""

import logging
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
import json

from .models import Row, RowStatus

logger = logging.getLogger(__name__)


class ValidationReport:
    """Generates validation reports."""
    
    def __init__(self, batch_id: str, file_name: str = None):
        self.batch_id = batch_id
        self.file_name = file_name
        self.generated_at = datetime.now()
    
    def generate_summary(self, 
                        valid_rows: List[Row], 
                        flagged_rows: List[Row]) -> Dict[str, Any]:
        """
        Generate validation summary report.
        
        Args:
            valid_rows: List of valid Row objects
            flagged_rows: List of flagged Row objects
        
        Returns:
            Summary dict
        """
        total = len(valid_rows) + len(flagged_rows)
        valid_pct = (len(valid_rows) / total * 100) if total > 0 else 0
        
        # Aggregate error types
        error_types = {}
        for row in flagged_rows:
            for error in row.validation_errors:
                rule_type = error.get('rule', 'unknown')
                error_types[rule_type] = error_types.get(rule_type, 0) + 1
        
        # Most common errors
        top_errors = sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            'batch_id': self.batch_id,
            'file_name': self.file_name,
            'generated_at': self.generated_at.isoformat(),
            'summary': {
                'total_rows': total,
                'valid_rows': len(valid_rows),
                'flagged_rows': len(flagged_rows),
                'valid_percentage': round(valid_pct, 2)
            },
            'error_distribution': dict(top_errors),
            'errors_by_field': self._aggregate_field_errors(flagged_rows)
        }
    
    def generate_detailed_report(self, flagged_rows: List[Row]) -> List[Dict[str, Any]]:
        """
        Generate detailed report for flagged rows.
        
        Args:
            flagged_rows: List of flagged Row objects
        
        Returns:
            List of row error details
        """
        details = []
        
        for row in flagged_rows:
            details.append({
                'row_id': row.row_id,
                'line_number': row.line_number,
                'errors': row.validation_errors,
                'canonical_data': row.canonical_data
            })
        
        return details
    
    @staticmethod
    def _aggregate_field_errors(flagged_rows: List[Row]) -> Dict[str, int]:
        """Aggregate errors by field."""
        field_errors = {}
        
        for row in flagged_rows:
            for error in row.validation_errors:
                field = error.get('field', 'unknown')
                field_errors[field] = field_errors.get(field, 0) + 1
        
        return field_errors
    
    def export_json(self, 
                   valid_rows: List[Row], 
                   flagged_rows: List[Row],
                   output_path: Path) -> Path:
        """
        Export validation report to JSON.
        
        Args:
            valid_rows: List of valid rows
            flagged_rows: List of flagged rows
            output_path: Path to save report
        
        Returns:
            Path to saved report
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        report = {
            'summary': self.generate_summary(valid_rows, flagged_rows),
            'flagged_details': self.generate_detailed_report(flagged_rows)
        }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f'Validation report exported to {output_path}')
        return output_path
    
    def export_csv(self, 
                  flagged_rows: List[Row],
                  output_path: Path) -> Path:
        """
        Export flagged rows to CSV for review.
        
        Args:
            flagged_rows: List of flagged rows
            output_path: Path to save CSV
        
        Returns:
            Path to saved CSV
        """
        import csv
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow(['row_id', 'line_number', 'field', 'rule', 'message'])
            
            # Rows
            for row in flagged_rows:
                for error in row.validation_errors:
                    writer.writerow([
                        row.row_id,
                        row.line_number,
                        error.get('field', ''),
                        error.get('rule', ''),
                        error.get('message', '')
                    ])
        
        logger.info(f'Flagged rows exported to {output_path}')
        return output_path
