"""
Detect file format using magic bytes, extension, and header inspection.
Deterministic format detection with fallback strategies.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class FormatDetector:
    """Detect file format deterministically."""
    
    # Magic bytes (file signatures)
    MAGIC_BYTES = {
        b'PK\x03\x04': 'xlsx',  # ZIP-based formats
        b'%PDF': 'pdf',
        b'<?xml': 'xml',
        b'<?XML': 'xml',
        b'[': 'json',
        b'{': 'json',
    }
    
    # Fallback by extension
    EXTENSION_MAP = {
        '.csv': 'csv',
        '.tsv': 'tsv',
        '.xlsx': 'xlsx',
        '.xls': 'xls',
        '.json': 'json',
        '.jsonl': 'jsonl',
        '.ndjson': 'jsonl',
        '.xml': 'xml',
        '.pdf': 'pdf',
        '.txt': 'txt',
    }
    
    @staticmethod
    def detect(file_path: Path) -> str:
        """
        Detect file format using multiple methods.
        
        Args:
            file_path: Path to file
        
        Returns:
            Format string ('csv', 'xlsx', 'json', 'xml', 'pdf', 'txt')
        """
        file_path = Path(file_path)
        
        # Method 1: Extension-based detection
        ext = file_path.suffix.lower()
        if ext in FormatDetector.EXTENSION_MAP:
            detected = FormatDetector.EXTENSION_MAP[ext]
            logger.debug(f"Detected format by extension: {detected}")
            return detected
        
        # Method 2: Magic bytes
        try:
            with open(file_path, 'rb') as f:
                magic = f.read(10)
            
            for signature, fmt in FormatDetector.MAGIC_BYTES.items():
                if magic.startswith(signature):
                    logger.debug(f"Detected format by magic bytes: {fmt}")
                    return fmt
        except (IOError, OSError) as e:
            logger.warning(f"Could not read file for magic bytes: {e}")
        
        # Method 3: Content inspection for CSV/TSV
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline()
            
            if '\t' in first_line:
                logger.debug("Detected format: tsv")
                return 'tsv'
            elif ',' in first_line:
                logger.debug("Detected format: csv")
                return 'csv'
        except Exception as e:
            logger.warning(f"Could not inspect content: {e}")
        
        # Default to txt
        logger.warning(f"Could not determine format for {file_path.name}, defaulting to txt")
        return 'txt'
