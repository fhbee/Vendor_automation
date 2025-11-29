"""
Export pipeline orchestrator.
Coordinates all exporters and manages export operations.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from .models import Row, RowStatus, BatchResult, FileStatus
from .metadata_store import MetadataStore
from .exporter import ExportStats
from .csv_exporter import CSVExporter
from .excel_exporter import ExcelExporter
from .json_exporter import JSONExporter
from .xml_exporter import XMLExporter
from .utils import generate_batch_id

logger = logging.getLogger(__name__)


class ExportPipeline:
    """Orchestrates export operations across all formats."""
    
    EXPORTERS = {
        'csv': CSVExporter,
        'excel': ExcelExporter,
        'json': JSONExporter,
        'xml': XMLExporter,
    }
    
    def __init__(self, metadata_store: MetadataStore, output_dir: Path):
        """
        Initialize export pipeline.
        
        Args:
            metadata_store: MetadataStore instance
            output_dir: Base output directory
        """
        self.metadata_store = metadata_store
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats = ExportStats()
    
    def export_rows(self, rows: List[Row], format: str, filename: str, 
                   **options) -> Path:
        """
        Export rows in specified format.
        
        Args:
            rows: List of Row objects
            format: Export format ('csv', 'excel', 'json', 'xml')
            filename: Output filename
            **options: Format-specific options
        
        Returns:
            Path to exported file
        """
        if format not in self.EXPORTERS:
            raise ValueError(f"Unsupported export format: {format}")
        
        exporter_class = self.EXPORTERS[format]
        exporter = exporter_class(self.output_dir)
        
        logger.info(f"Exporting {len(rows)} rows to {format.upper()}: {filename}")
        
        export_path = exporter.export(rows, filename, **options)
        self.stats.add_export(rows)
        
        return export_path
    
    def export_file(self, file_id: str, format: str, filename: str,
                   **options) -> Dict[str, Any]:
        """
        Export all rows from file.
        
        Args:
            file_id: File ID to export
            format: Export format
            filename: Output filename
            **options: Format-specific options
        
        Returns:
            Export result dict with paths and stats
        """
        rows = self.metadata_store.get_rows_by_file(file_id)
        
        if not rows:
            logger.warning(f"No rows found for file {file_id}")
            return {'status': 'NO_DATA'}
        
        logger.info(f"Exporting {len(rows)} rows from file {file_id}")
        
        # Export valid rows
        valid_rows = [r for r in rows if r.status == RowStatus.VALID]
        valid_path = None
        if valid_rows:
            valid_path = self.export_rows(valid_rows, format, 
                                         filename.replace('.', '_valid.'),
                                         status=RowStatus.VALID, **options)
        
        # Export flagged rows
        flagged_rows = [r for r in rows if r.status == RowStatus.FLAGGED]
        flagged_path = None
        if flagged_rows:
            flagged_path = self.export_rows(flagged_rows, format,
                                           filename.replace('.', '_flagged.'),
                                           status=RowStatus.FLAGGED, **options)
        
        return {
            'status': 'SUCCESS',
            'file_id': file_id,
            'total_rows': len(rows),
            'valid_rows': len(valid_rows),
            'flagged_rows': len(flagged_rows),
            'valid_export': str(valid_path) if valid_path else None,
            'flagged_export': str(flagged_path) if flagged_path else None,
        }
    
    def export_all_files(self, format: str, output_subdir: str = None,
                        **options) -> BatchResult:
        """
        Export all processed files.
        
        Args:
            format: Export format
            output_subdir: Optional subdirectory for exports
            **options: Format-specific options
        
        Returns:
            BatchResult with export statistics
        """
        batch_id = generate_batch_id()
        batch_result = BatchResult(
            batch_id=batch_id,
            started_at=datetime.now()
        )
        
        try:
            # Get all files from metadata
            files = self.metadata_store.get_all_files()
            
            if not files:
                logger.warning("No files to export")
                return batch_result
            
            logger.info(f"Exporting {len(files)} files to {format.upper()}")
            
            export_dir = self.output_dir
            if output_subdir:
                export_dir = self.output_dir / output_subdir
                export_dir.mkdir(parents=True, exist_ok=True)
            
            # Export each file
            for file_meta in files:
                try:
                    filename = f"{file_meta.filename.split('.')[0]}.{format}"
                    
                    result = self.export_file(file_meta.file_id, format, 
                                            filename, **options)
                    
                    batch_result.file_results.append(result)
                    batch_result.total_rows += result.get('total_rows', 0)
                
                except Exception as e:
                    logger.error(f"Error exporting file {file_meta.file_id}: {e}")
                    batch_result.errors.append(str(e))
            
            batch_result.completed_at = datetime.now()
            batch_result.status = FileStatus.SUCCESS if not batch_result.errors else FileStatus.PARTIAL_SUCCESS
            
            logger.info(f"Export batch {batch_id} complete: {batch_result.total_rows} rows")
            
        except Exception as e:
            logger.error(f"Error in export batch: {e}")
            batch_result.errors.append(str(e))
            batch_result.status = FileStatus.FAILED
        
        return batch_result
    
    def get_export_stats(self) -> Dict[str, Any]:
        """Get export statistics."""
        return self.stats.to_dict()
