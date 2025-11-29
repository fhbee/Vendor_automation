"""
Local filesystem connector for reading files.
Implements connector interface for local file ingestion.
"""

import logging
from pathlib import Path
from datetime import datetime
from .models import FileMeta, FileStatus
from .utils import compute_file_hash, archive_raw_file

logger = logging.getLogger(__name__)


class LocalFSConnector:
    """Connector for reading files from local filesystem."""
    
    def __init__(self, archive_dir: Path):
        """
        Initialize connector.
        
        Args:
            archive_dir: Directory to archive raw files
        """
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
    
    def fetch(self, file_path: Path) -> FileMeta:
        """
        Fetch file from filesystem and create metadata.
        
        Args:
            file_path: Path to source file
        
        Returns:
            FileMeta object with computed hash and metadata
        
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Compute file hash for deterministic ID
        file_hash = compute_file_hash(file_path)
        logger.debug(f"Computed hash for {file_path.name}: {file_hash[:8]}...")
        
        # Get file info
        stat = file_path.stat()
        file_size = stat.st_size
        
        # Detect file type from extension
        file_type = file_path.suffix.lstrip('.').lower() or 'unknown'
        
        # Archive raw file
        archive_path = archive_raw_file(file_path, self.archive_dir, file_hash)
        
        # Create metadata
        file_meta = FileMeta(
            file_id=file_hash,
            filename=file_path.name,
            path=str(file_path.resolve()),
            file_type=file_type,
            size_bytes=file_size,
            created_at=datetime.now(),
            status=FileStatus.PENDING,
            raw_copy_path=str(archive_path),
        )
        
        logger.info(f"Fetched file: {file_meta.filename} ({file_meta.file_id[:8]}...)")
        return file_meta
