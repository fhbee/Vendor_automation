"""
XML file parser with streaming support.
Auto-detects repeating elements and converts to dictionaries.
"""

import logging
from pathlib import Path
from typing import Iterator, List, Dict, Any
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


class XMLParser:
    """Parse XML files with row extraction."""
    
    def __init__(self, chunk_size: int = 500):
        """
        Initialize XML parser.
        
        Args:
            chunk_size: Number of rows per chunk
        """
        self.chunk_size = chunk_size
    
    def parse(self, file_path: Path, row_element: str = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Parse XML file and yield chunks of rows.
        Expects XML to have repeating elements representing rows.
        
        Args:
            file_path: Path to XML file
            row_element: Tag name of repeating row element (auto-detect if None)
        
        Yields:
            Lists of row dicts (chunk_size rows per iteration)
        """
        file_path = Path(file_path)
        logger.info(f"Parsing XML: {file_path.name}")
        
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Auto-detect row element if not provided
            if row_element is None:
                row_element = self._detect_row_element(root)
                logger.info(f"Auto-detected row element: {row_element}")
            
            # Extract rows
            chunk = []
            for row_elem in root.iter(row_element):
                row_dict = self._element_to_dict(row_elem)
                chunk.append(row_dict)
                
                if len(chunk) >= self.chunk_size:
                    logger.debug(f"Yielding chunk with {len(chunk)} rows")
                    yield chunk
                    chunk = []
            
            if chunk:
                logger.debug(f"Yielding final chunk with {len(chunk)} rows")
                yield chunk
        
        except Exception as e:
            logger.error(f"Error parsing XML: {e}")
            raise
    
    @staticmethod
    def _detect_row_element(root: ET.Element) -> str:
        """Auto-detect repeating element name."""
        children_tags = {}
        for child in root:
            tag = child.tag
            children_tags[tag] = children_tags.get(tag, 0) + 1
        
        # Most common child element is likely the row
        if children_tags:
            most_common = max(children_tags.items(), key=lambda x: x[1])[0]
            return most_common
        
        return 'row'
    
    @staticmethod
    def _element_to_dict(elem: ET.Element) -> Dict[str, Any]:
        """Convert XML element to dict."""
        result = {}
        
        # Element text
        if elem.text and elem.text.strip():
            result['_text'] = elem.text.strip()
        
        # Attributes
        if elem.attrib:
            for key, value in elem.attrib.items():
                result[key] = value
        
        # Child elements
        for child in elem:
            child_data = XMLParser._element_to_dict(child)
            if child.tag in result:
                # If tag already exists, convert to list
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result
