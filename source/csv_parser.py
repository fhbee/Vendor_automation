"""
CSV/TSV file parser with streaming support.
Handles encoding issues and yields rows in chunks.
"""

import logging
from pathlib import Path
from typing import Iterator, List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


class CSVParser:
    """Parse CSV/TSV files with chunking."""
    
    def __init__(self, chunk_size: int = 500, delimiter: str = ',', encoding: str = 'utf-8'):
        """
        Initialize CSV parser.
        
        Args:
            chunk_size: Number of rows per chunk
            delimiter: Field delimiter
            encoding: File encoding
        """
        self.chunk_size = chunk_size
        self.delimiter = delimiter
        self.encoding = encoding
    
    def parse(self, file_path: Path) -> Iterator[List[Dict[str, Any]]]:
        """
        Parse CSV file and yield chunks of rows.
        
        Args:
            file_path: Path to CSV file
        
        Yields:
            Lists of row dicts (chunk_size rows per iteration)
        """
        file_path = Path(file_path)
        logger.info(f"Parsing CSV: {file_path.name}")
        
        try:
            # Use pandas for robust parsing
            for chunk_df in pd.read_csv(
                file_path,
                delimiter=self.delimiter,
                dtype=str,  # Keep as strings initially
                chunksize=self.chunk_size,
                encoding=self.encoding,
                on_bad_lines='warn',
                engine='python'
            ):
                # Convert chunk to list of dicts
                chunk_rows = chunk_df.where(pd.notna(chunk_df), None).to_dict('records')
                logger.debug(f"Yielding chunk with {len(chunk_rows)} rows")
                yield chunk_rows
        
        except UnicodeDecodeError:
            logger.warning(f"UTF-8 decode failed, trying latin-1")
            self.encoding = 'latin-1'
            yield from self.parse(file_path)
        
        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            raise
