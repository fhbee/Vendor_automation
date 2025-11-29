"""
Excel (XLSX) file parser with streaming support.
Row-by-row processing to handle large files efficiently.
"""

import logging
from pathlib import Path
from typing import Iterator, List, Dict, Any
import openpyxl

logger = logging.getLogger(__name__)


class XLSXParser:
    """Parse XLSX files with row-by-row streaming."""
    
    def __init__(self, chunk_size: int = 500):
        """
        Initialize XLSX parser.
        
        Args:
            chunk_size: Number of rows per chunk
        """
        self.chunk_size = chunk_size
    
    def parse(self, file_path: Path, sheet_name: str = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Parse XLSX file and yield chunks of rows.
        
        Args:
            file_path: Path to XLSX file
            sheet_name: Specific sheet to parse (default: first sheet)
        
        Yields:
            Lists of row dicts (chunk_size rows per iteration)
        """
        file_path = Path(file_path)
        logger.info(f"Parsing XLSX: {file_path.name}")
        
        try:
            workbook = openpyxl.load_workbook(file_path, data_only=True)
            
            # Get sheet
            if sheet_name:
                worksheet = workbook[sheet_name]
            else:
                worksheet = workbook.active
            
            logger.info(f"Reading sheet: {worksheet.title}")
            
            # Extract header row
            header = None
            chunk = []
            row_num = 0
            
            for row_idx, row in enumerate(worksheet.iter_rows(values_only=True), start=1):
                row_num = row_idx
                
                # First row is header
                if header is None:
                    header = [str(cell) if cell is not None else f'Column{i}' 
                             for i, cell in enumerate(row)]
                    logger.debug(f"Detected header: {header}")
                    continue
                
                # Build row dict
                row_dict = {}
                for col_idx, value in enumerate(row):
                    col_name = header[col_idx] if col_idx < len(header) else f'Column{col_idx}'
                    row_dict[col_name] = str(value) if value is not None else None
                
                chunk.append(row_dict)
                
                # Yield when chunk full
                if len(chunk) >= self.chunk_size:
                    logger.debug(f"Yielding chunk with {len(chunk)} rows")
                    yield chunk
                    chunk = []
            
            # Yield remaining rows
            if chunk:
                logger.debug(f"Yielding final chunk with {len(chunk)} rows")
                yield chunk
            
            logger.info(f"Parsed {row_num} rows total")
            workbook.close()
        
        except Exception as e:
            logger.error(f"Error parsing XLSX: {e}")
            raise
