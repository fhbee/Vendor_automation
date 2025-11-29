"""
JSON file parser with streaming support.
Handles both JSON array and JSONL (newline-delimited) formats.
"""

import json
import logging
from pathlib import Path
from typing import Iterator, List, Dict, Any
import ijson

logger = logging.getLogger(__name__)


class JSONParser:
    """Parse JSON files (array or JSONL format) with streaming."""
    
    def __init__(self, chunk_size: int = 500):
        """
        Initialize JSON parser.
        
        Args:
            chunk_size: Number of rows per chunk
        """
        self.chunk_size = chunk_size
    
    def parse(self, file_path: Path) -> Iterator[List[Dict[str, Any]]]:
        """
        Parse JSON file and yield chunks of rows.
        Supports: JSON array [{...}, {...}] or JSONL (line-delimited)
        
        Args:
            file_path: Path to JSON file
        
        Yields:
            Lists of row dicts (chunk_size rows per iteration)
        """
        file_path = Path(file_path)
        logger.info(f"Parsing JSON: {file_path.name}")
        
        try:
            # Detect if JSONL or JSON array
            is_jsonl = self._is_jsonl(file_path)
            
            if is_jsonl:
                yield from self._parse_jsonl(file_path)
            else:
                yield from self._parse_json_array(file_path)
        
        except Exception as e:
            logger.error(f"Error parsing JSON: {e}")
            raise
    
    def _is_jsonl(self, file_path: Path) -> bool:
        """Check if file is JSONL format (heuristic)."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
            
            return first_line.startswith('{') or first_line.startswith('[')
        except Exception:
            return False
    
    def _parse_jsonl(self, file_path: Path) -> Iterator[List[Dict[str, Any]]]:
        """Parse JSONL (newline-delimited JSON)."""
        chunk = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    row_dict = json.loads(line)
                    if isinstance(row_dict, dict):
                        chunk.append(row_dict)
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line {line_num}: {e}")
                
                if len(chunk) >= self.chunk_size:
                    logger.debug(f"Yielding chunk with {len(chunk)} rows")
                    yield chunk
                    chunk = []
        
        if chunk:
            logger.debug(f"Yielding final chunk with {len(chunk)} rows")
            yield chunk
    
    def _parse_json_array(self, file_path: Path) -> Iterator[List[Dict[str, Any]]]:
        """Parse JSON array using ijson for streaming large files."""
        chunk = []
        
        with open(file_path, 'rb') as f:
            # Use ijson to stream objects from array
            for item in ijson.items(f, 'item'):
                if isinstance(item, dict):
                    # Convert all values to strings for consistency
                    str_item = {k: str(v) if v is not None else None for k, v in item.items()}
                    chunk.append(str_item)
                
                if len(chunk) >= self.chunk_size:
                    logger.debug(f"Yielding chunk with {len(chunk)} rows")
                    yield chunk
                    chunk = []
        
        if chunk:
            logger.debug(f"Yielding final chunk with {len(chunk)} rows")
            yield chunk
