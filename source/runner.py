"""
Main runner for batch processing.
Discovers files, groups into batches, orchestrates pipeline.
Implements idempotent re-processing with deterministic hashing.
"""

import logging
import argparse
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from .metadata_store import MetadataStore
from .config_loader import ConfigLoader
from .models import FileMeta, FileStatus, BatchResult
from .utils import generate_batch_id, compute_file_hash
from .local_fs import LocalFSConnector
from .ingestion_pipeline import IngestionPipeline
from .normalization_pipeline import NormalizationPipeline
from .validation_pipeline import ValidationPipeline
from .export_pipeline import ExportPipeline
from .reporter import Reporter

logger = logging.getLogger(__name__)


class BatchRunner:
    """Orchestrates batch discovery, grouping, and pipeline execution."""
    
    def __init__(self, config_path: Path, db_path: Path):
        """
        Initialize batch runner.
        
        Args:
            config_path: Path to global config.yaml
            db_path: Path to SQLite metadata database
        """
        self.config = ConfigLoader(config_path.parent)
        self.metadata_store = MetadataStore(db_path)
        self.batch_id = generate_batch_id()
        
        logger.info(f"BatchRunner initialized with batch_id: {self.batch_id}")
    
    def discover_files(self, input_dir: Path, force: bool = False) -> List[Path]:
        """
        Discover unprocessed files in directory.
        
        Args:
            input_dir: Directory to scan
            force: If True, reprocess all files; otherwise skip existing
        
        Returns:
            List of file paths to process
        """
        input_dir = Path(input_dir)
        files = []
        
        for file_path in input_dir.glob('**/*.*'):
            if not file_path.is_file():
                continue
            
            # Compute hash
            file_hash = compute_file_hash(file_path)
            
            # Check if already processed
            existing = self.metadata_store.get_file(file_hash)
            if existing and existing.status == FileStatus.SUCCESS and not force:
                logger.info(f"Skipping {file_path.name} (already processed)")
                continue
            
            files.append(file_path)
        
        logger.info(f"Discovered {len(files)} files to process")
        return files
    
    def process_batch(self, input_dir: Path, force: bool = False,
                     config_name: str = "default") -> BatchResult:
        """
        Process entire batch from directory.
        
        Args:
            input_dir: Input directory
            force: Force reprocessing
            config_name: Vendor config name
        
        Returns:
            BatchResult with statistics
        """
        batch_result = BatchResult(
            batch_id=self.batch_id,
            started_at=datetime.now()
        )
        
        try:
            # Discover files
            files = self.discover_files(input_dir, force)
            if not files:
                logger.warning("No files to process")
                return batch_result
            
            global_config = self.config.load_global_config()
            
            # Initialize pipelines
            connector = LocalFSConnector(Path(global_config.get('archive_dir', './archive')))
            ingestion = IngestionPipeline(self.metadata_store, Path(global_config.get('archive_dir', './archive')))
            normalization = NormalizationPipeline(
                self.metadata_store,
                self.config.load_canonical_schema(),
                self.config.load_vendor_mapping_rules(config_name)
            )
            validation = ValidationPipeline(
                self.metadata_store,
                validation_rules=self.config.load_vendor_validation_rules(config_name)
            )
            export_pipeline = ExportPipeline(
                self.metadata_store,
                Path(global_config.get('export_dir', './exports'))
            )
            
            # Process each file
            for file_path in files:
                try:
                    logger.info(f"Processing file: {file_path.name}")
                    
                    # Phase 2: Ingest
                    file_meta, rows = ingestion.process_file(file_path)
                    batch_result.file_results.append(file_meta)
                    
                    # Phase 3: Normalize + Map
                    for chunk in rows:
                        normalized = normalization.process_batch(chunk)
                    
                    # Phase 4: Validate
                    all_rows = self.metadata_store.get_rows_by_file(file_meta.file_id)
                    valid, flagged = validation.validate_batch(all_rows)
                    
                    # Phase 5: Export
                    export_result = export_pipeline.export_file(
                        file_meta.file_id,
                        format='csv',
                        filename=f"{file_path.stem}.csv"
                    )
                    
                    batch_result.total_rows += len(all_rows)
                    batch_result.valid_rows += len(valid)
                    batch_result.flagged_rows += len(flagged)
                
                except Exception as e:
                    logger.error(f"Error processing {file_path.name}: {e}")
                    batch_result.errors.append(str(e))
            
            batch_result.completed_at = datetime.now()
            batch_result.status = FileStatus.SUCCESS if not batch_result.errors else FileStatus.PARTIAL_SUCCESS
            
            # Generate report
            reporter = Reporter(self.batch_id)
            report_result = reporter.generate_report(valid, flagged)
            batch_result.report_path = report_result.get('report_json_path')
            
            logger.info(f"Batch {self.batch_id} complete: {batch_result.total_rows} rows processed")
            
        except Exception as e:
            logger.error(f"Fatal error in batch processing: {e}")
            batch_result.errors.append(str(e))
            batch_result.status = FileStatus.FAILED
        
        return batch_result


def main():
    """Main entry point for runner."""
    parser = argparse.ArgumentParser(description="Vendor data automation pipeline runner")
    
    parser.add_argument('--watch-folder', type=Path, help='Watch folder for new files')
    parser.add_argument('--process-once', action='store_true', help='Process once and exit')
    parser.add_argument('--input', type=Path, help='Input directory')
    parser.add_argument('--config', type=Path, default=Path('./config.yaml'), help='Config path')
    parser.add_argument('--db', type=Path, default=Path('./metadata/auto.db'), help='Database path')
    parser.add_argument('--force', action='store_true', help='Force reprocessing')
    parser.add_argument('--vendor', type=str, default='default', help='Vendor config name')
    
    args = parser.parse_args()
    
    # Initialize runner
    runner = BatchRunner(args.config, args.db)
    
    if args.process_once and args.input:
        # One-time batch processing
        result = runner.process_batch(args.input, force=args.force, config_name=args.vendor)
        print(f"Batch {result.batch_id}: {result.total_rows} rows, {result.valid_rows} valid, {result.flagged_rows} flagged")
    else:
        print("Use --process-once --input <dir> for one-time batch processing")


if __name__ == '__main__':
    main()
