"""
Ingestion pipeline orchestrator.
Coordinates connector, parser, and metadata flow.
"""

import logging
from pathlib import Path
from typing import Iterator, List, Dict, Any, Tuple
from datetime import datetime

from .models import FileMeta, Row, RowStatus, FileStatus, BatchResult
from .metadata_store import MetadataStore
from .local_fs import LocalFSConnector
from .parsers_init import FormatDetector, CSVParser, XLSXParser, JSONParser, XMLParser
from .utils import generate_batch_id, generate_row_id

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """Orchestrates file ingestion and parsing."""
    
    PARSERS = {
        'csv': CSVParser,
        'tsv': lambda: CSVParser(delimiter='\t'),
        'xlsx': XLSXParser,
        'json': JSONParser,
        'jsonl': JSONParser,
        'xml': XMLParser,
    }
    
    def __init__(self, 
                 metadata_store: MetadataStore,
                 archive_dir: Path,
                 chunk_size: int = 500):
        """
        Initialize ingestion pipeline.
        
        Args:
            metadata_store: MetadataStore instance
            archive_dir: Directory to archive raw files
            chunk_size: Rows per chunk
        """
        self.metadata_store = metadata_store
        self.archive_dir = Path(archive_dir)
        self.chunk_size = chunk_size
        self.connector = LocalFSConnector(archive_dir)
    
    def process_file(self, file_path: Path, vendor_id: str = None) -> Tuple[FileMeta, Iterator[List[Row]]]:
        """
        Process single file: ingest, detect format, parse.
        
        Args:
            file_path: Path to input file
            vendor_id: Optional vendor identifier
        
        Returns:
            Tuple of (FileMeta, Iterator[List[Row]])
        """
        file_path = Path(file_path)
        logger.info(f"Processing file: {file_path.name}")
        
        # Step 1: Fetch & create metadata
        file_meta = self.connector.fetch(file_path)
        
        # Check if already processed (idempotency)
        existing_meta = self.metadata_store.get_file(file_meta.file_id)
        if existing_meta and existing_meta.status == FileStatus.SUCCESS:
            logger.info(f"File already processed: {file_meta.file_id[:8]}...")
            # Return cached results
            existing_rows = self.metadata_store.get_rows_by_file(file_meta.file_id)
            row_chunks = self._chunk_rows(existing_rows)
            return file_meta, row_chunks
        
        # Update status to processing
        file_meta.status = FileStatus.PROCESSING
        self.metadata_store.insert_file(file_meta)
        
        # Step 2: Detect format
        file_format = FormatDetector.detect(file_path)
        logger.info(f"Detected format: {file_format}")
        
        # Step 3: Parse & yield Row objects
        try:
            row_iterator = self._parse_and_create_rows(file_path, file_format, file_meta)
            
            # Mark as success
            file_meta.status = FileStatus.SUCCESS
            file_meta.processed_at = datetime.now()
            self.metadata_store.insert_file(file_meta)
            
            return file_meta, row_iterator
        
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            file_meta.status = FileStatus.FAILED
            file_meta.error_message = str(e)
            self.metadata_store.insert_file(file_meta)
            raise
    
    def _parse_and_create_rows(self, 
                               file_path: Path, 
                               file_format: str,
                               file_meta: FileMeta) -> Iterator[List[Row]]:
        """Parse file and create Row objects."""
        
        # Get parser
        parser_class = self.PARSERS.get(file_format)
        if not parser_class:
            raise ValueError(f"Unsupported format: {file_format}")
        
        if callable(parser_class):
            parser = parser_class()
        else:
            parser = parser_class(chunk_size=self.chunk_size)
        
        # Parse and wrap in Row objects
        line_number = 0
        for chunk in parser.parse(file_path):
            row_chunk = []
            for raw_data in chunk:
                line_number += 1
                row = Row(
                    row_id=generate_row_id(file_meta.file_id, line_number),
                    file_id=file_meta.file_id,
                    line_number=line_number,
                    raw_data=raw_data,
                    status=RowStatus.PENDING
                )
                row_chunk.append(row)
            
            # Store rows
            for row in row_chunk:
                self.metadata_store.insert_row(row)
            
            yield row_chunk
        
        file_meta.row_count = line_number
        self.metadata_store.insert_file(file_meta)
    
    def _chunk_rows(self, rows: List[Row]) -> Iterator[List[Row]]:
        """Chunk rows for yielding."""
        for i in range(0, len(rows), self.chunk_size):
            yield rows[i:i + self.chunk_size]
    
    def process_directory(self, input_dir: Path, vendor_id: str = None) -> BatchResult:
        """
        Process all files in directory.
        
        Args:
            input_dir: Directory containing input files
            vendor_id: Optional vendor identifier
        
        Returns:
            BatchResult with aggregated statistics
        """
        input_dir = Path(input_dir)
        batch_id = generate_batch_id()
        
        logger.info(f"Starting batch {batch_id} for {input_dir}")
        
        batch_result = BatchResult(
            batch_id=batch_id,
            started_at=datetime.now()
        )
        
        # Discover all files
        files = list(input_dir.glob('**/*.*'))
        if not files:
            logger.warning(f"No files found in {input_dir}")
            return batch_result
        
        # Process each file
        for file_path in files:
            if file_path.is_file():
                try:
                    file_meta, rows = self.process_file(file_path, vendor_id)
                    batch_result.file_results.append(file_meta)
                    
                    # Count rows
                    for chunk in rows:
                        batch_result.total_rows += len(chunk)
                
                except Exception as e:
                    logger.error(f"Failed to process {file_path.name}: {e}")
                    batch_result.errors.append(str(e))
        
        batch_result.completed_at = datetime.now()
        batch_result.status = FileStatus.SUCCESS if not batch_result.errors else FileStatus.PARTIAL_SUCCESS
        
        logger.info(f"Batch {batch_id} complete: {batch_result.total_rows} rows")
        return batch_result
