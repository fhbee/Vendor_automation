"""
Reporter module for batch summary and AI-assisted reporting.
Generates JSON and human-readable reports with statistics and insights.
"""

import logging
import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

from .models import Row, RowStatus

logger = logging.getLogger(__name__)


class Reporter:
    """Generates batch reports with statistics and AI summaries."""
    
    def __init__(self, batch_id: str, output_dir: Path = Path('./reports')):
        """
        Initialize reporter.
        
        Args:
            batch_id: Batch ID
            output_dir: Output directory for reports
        """
        self.batch_id = batch_id
        self.output_dir = Path(output_dir) / batch_id
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_report(self, valid_rows: List[Row], 
                       flagged_rows: List[Row]) -> Dict[str, Any]:
        """
        Generate comprehensive batch report.
        
        Args:
            valid_rows: List of valid rows
            flagged_rows: List of flagged rows
        
        Returns:
            Dict with report paths
        """
        total_rows = len(valid_rows) + len(flagged_rows)
        valid_pct = (len(valid_rows) / total_rows * 100) if total_rows > 0 else 0
        
        # Aggregate statistics
        error_stats = self._aggregate_errors(flagged_rows)
        field_errors = self._aggregate_field_errors(flagged_rows)
        
        # Build JSON report
        json_report = {
            'batch_id': self.batch_id,
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_rows': total_rows,
                'valid_rows': len(valid_rows),
                'flagged_rows': len(flagged_rows),
                'valid_percentage': round(valid_pct, 2)
            },
            'error_distribution': error_stats,
            'errors_by_field': field_errors,
            'sample_flagged_rows': self._sample_flagged_rows(flagged_rows, max_samples=10)
        }
        
        # Write JSON report
        json_path = self.output_dir / 'report.json'
        with open(json_path, 'w') as f:
            json.dump(json_report, f, indent=2)
        
        logger.info(f"JSON report written to {json_path}")
        
        # Generate human-readable summary
        text_summary = self._generate_text_summary(json_report)
        
        text_path = self.output_dir / 'report.txt'
        with open(text_path, 'w') as f:
            f.write(text_summary)
        
        logger.info(f"Text report written to {text_path}")
        
        # Write manifest
        manifest_path = self.output_dir / 'manifest.json'
        manifest = {
            'batch_id': self.batch_id,
            'generated_at': datetime.now().isoformat(),
            'report_json': str(json_path),
            'report_text': str(text_path),
            'statistics': json_report['summary']
        }
        
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        return {
            'report_json_path': str(json_path),
            'report_text_path': str(text_path),
            'manifest_path': str(manifest_path)
        }
    
    def _aggregate_errors(self, flagged_rows: List[Row]) -> Dict[str, int]:
        """Aggregate error types."""
        error_types = {}
        
        for row in flagged_rows:
            for error in row.validation_errors:
                rule_type = error.get('rule', 'unknown')
                error_types[rule_type] = error_types.get(rule_type, 0) + 1
        
        # Sort by frequency
        return dict(sorted(error_types.items(), 
                          key=lambda x: x[1], reverse=True)[:5])  # Top 5
    
    def _aggregate_field_errors(self, flagged_rows: List[Row]) -> Dict[str, int]:
        """Aggregate errors by field."""
        field_errors = {}
        
        for row in flagged_rows:
            for error in row.validation_errors:
                field = error.get('field', 'unknown')
                field_errors[field] = field_errors.get(field, 0) + 1
        
        return field_errors
    
    def _sample_flagged_rows(self, flagged_rows: List[Row], 
                            max_samples: int = 10) -> List[Dict[str, Any]]:
        """Extract sample flagged rows."""
        samples = []
        
        for row in flagged_rows[:max_samples]:
            samples.append({
                'row_id': row.row_id[:8],
                'line_number': row.line_number,
                'errors': row.validation_errors,
                'canonical_data': row.canonical_data
            })
        
        return samples
    
    def _generate_text_summary(self, json_report: Dict[str, Any]) -> str:
        """Generate human-readable text summary."""
        summary = json_report['summary']
        
        text = f"""
{'='*70}
BATCH REPORT
{'='*70}

Batch ID: {json_report['batch_id']}
Generated: {json_report['generated_at']}

SUMMARY
-------
Total Rows Processed: {summary['total_rows']:,}
Valid Rows: {summary['valid_rows']:,} ({summary['valid_percentage']:.1f}%)
Flagged Rows: {summary['flagged_rows']:,} ({100 - summary['valid_percentage']:.1f}%)

TOP ERRORS
----------
"""
        for error_type, count in json_report['error_distribution'].items():
            text += f"  {error_type}: {count}\n"
        
        text += f"""
ERRORS BY FIELD
---------------
"""
        for field, count in json_report['errors_by_field'].items():
            text += f"  {field}: {count}\n"
        
        text += f"""
ACTION ITEMS
------------
1. Review {summary['flagged_rows']:,} flagged rows using: python -m cli view-flagged --batch {json_report['batch_id']}
2. Approve or reject flagged rows after review
3. Export report to CSV for stakeholder review

{'='*70}
"""
        return text
