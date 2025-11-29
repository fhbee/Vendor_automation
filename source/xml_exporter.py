"""
XML exporter for valid and flagged rows.
Generates well-formed XML with configurable structure.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import xml.etree.ElementTree as ET
from xml.dom import minidom

from .models import Row, RowStatus
from .exporter import Exporter

logger = logging.getLogger(__name__)


class XMLExporter(Exporter):
    """Export rows to XML format."""
    
    def export(self, rows: List[Row], filename: str, **options) -> Path:
        """
        Export rows to XML.
        
        Args:
            rows: List of Row objects
            filename: Output filename
            **options:
                - status: RowStatus filter (VALID, FLAGGED, None)
                - fields: List of fields to include
                - include_metadata: Include row metadata
                - include_errors: Include validation errors
                - root_element: Root element name (default: 'data')
                - row_element: Row element name (default: 'row')
                - pretty_print: Pretty print XML
        
        Returns:
            Path to exported XML file
        """
        status = options.get('status')
        fields = options.get('fields')
        include_metadata = options.get('include_metadata', False)
        include_errors = options.get('include_errors', True)
        root_element = options.get('root_element', 'data')
        row_element = options.get('row_element', 'row')
        pretty_print = options.get('pretty_print', True)
        
        filtered_rows = self._filter_rows(rows, status)
        
        if not filtered_rows:
            logger.warning(f"No rows to export for status {status}")
            return None
        
        output_path = self.output_dir / filename
        
        try:
            # Create root element
            root = ET.Element(root_element)
            root.set('count', str(len(filtered_rows)))
            root.set('status', status.value if status else 'mixed')
            
            # Add rows
            for row in filtered_rows:
                row_data = self._extract_fields(row, fields)
                
                if include_metadata:
                    row_data = self._add_metadata(row_data, row, include_errors)
                
                row_elem = ET.SubElement(root, row_element)
                
                # Add fields as child elements
                for field, value in row_data.items():
                    field_elem = ET.SubElement(row_elem, field.replace(' ', '_'))
                    field_elem.text = str(value) if value is not None else ''
            
            # Pretty print if enabled
            xml_string = ET.tostring(root, encoding='unicode')
            if pretty_print:
                dom = minidom.parseString(xml_string)
                xml_string = dom.toprettyxml(indent="  ")
                # Remove extra blank lines
                xml_string = '\n'.join([line for line in xml_string.split('\n') 
                                       if line.strip()])
            
            # Write to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(xml_string)
            
            self._log_export(filename, len(filtered_rows), status)
            return output_path
        
        except Exception as e:
            logger.error(f"Error exporting XML: {e}")
            raise
