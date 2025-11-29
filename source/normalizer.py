"""
Data normalization module.
Standardizes raw data before field mapping.
"""

import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional
from .models import Row, RowStatus

logger = logging.getLogger(__name__)


class Normalizer:
    """Normalize raw data to standard formats."""
    
    # Common date formats to try
    DATE_FORMATS = [
        '%Y-%m-%d',           # ISO8601
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%m-%d-%Y',
        '%m/%d/%Y',
        '%Y%m%d',
        '%d.%m.%Y',
        '%Y-%m-%d %H:%M:%S',  # With time
        '%d/%m/%Y %H:%M:%S',
    ]
    
    # Decimal separator patterns
    DECIMAL_SEPARATORS = {
        '.': '.',  # US: 1,000.50
        ',': '.',  # EU: 1.000,50
    }
    
    @staticmethod
    def normalize_row(raw_row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a row of raw data.
        
        Args:
            raw_row: Raw data dictionary
        
        Returns:
            Normalized data dictionary
        """
        normalized = {}
        
        for key, value in raw_row.items():
            if value is None:
                normalized[key] = None
            elif isinstance(value, str):
                # Trim whitespace
                value = value.strip()
                normalized[key] = value if value else None
            else:
                normalized[key] = value
        
        return normalized
    
    @staticmethod
    def normalize_date(date_str: str, source_format: Optional[str] = None) -> Optional[str]:
        """
        Normalize date to ISO8601 format (YYYY-MM-DD).
        
        Args:
            date_str: Date string
            source_format: Expected source format (optional)
        
        Returns:
            ISO8601 date string or None if parse fails
        """
        if not date_str or not isinstance(date_str, str):
            return None
        
        date_str = date_str.strip()
        
        # If already ISO8601, return
        if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]
        
        # Try source format first
        if source_format:
            try:
                parsed = datetime.strptime(date_str, source_format)
                return parsed.strftime('%Y-%m-%d')
            except ValueError:
                pass
        
        # Try known formats
        for fmt in Normalizer.DATE_FORMATS:
            try:
                parsed = datetime.strptime(date_str, fmt)
                return parsed.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    @staticmethod
    def normalize_decimal(value: Any, 
                         decimal_separator: Optional[str] = None,
                         thousands_separator: Optional[str] = None) -> Optional[str]:
        """
        Normalize decimal number to string with '.' as separator.
        
        Args:
            value: Numeric or string value
            decimal_separator: '.' or ',' (auto-detect if None)
            thousands_separator: ',' or '.' (auto-detect if None)
        
        Returns:
            Normalized decimal string or None
        """
        if value is None:
            return None
        
        if isinstance(value, (int, float, Decimal)):
            return str(value)
        
        if not isinstance(value, str):
            return None
        
        value = value.strip()
        if not value:
            return None
        
        # Auto-detect separators
        if decimal_separator is None or thousands_separator is None:
            decimal_separator, thousands_separator = Normalizer._detect_separators(value)
        
        # Remove thousands separator
        if thousands_separator:
            value = value.replace(thousands_separator, '')
        
        # Replace decimal separator with '.'
        if decimal_separator and decimal_separator != '.':
            value = value.replace(decimal_separator, '.')
        
        # Validate and convert
        try:
            dec = Decimal(value)
            return str(dec)
        except (InvalidOperation, ValueError):
            logger.warning(f"Could not parse decimal: {value}")
            return None
    
    @staticmethod
    def _detect_separators(value: str) -> tuple:
        """Auto-detect decimal and thousands separators."""
        # If last char is . or ,, likely decimal
        last_sep = None
        for char in ['.', ',']:
            if char in value:
                last_sep = char
        
        if not last_sep:
            return '.', None
        
        # Count occurrences
        dot_count = value.count('.')
        comma_count = value.count(',')
        
        # If both present, rightmost is decimal
        if dot_count > 0 and comma_count > 0:
            last_char_sep = value.rfind('.') > value.rfind(',')
            decimal_sep = '.' if last_char_sep else ','
            thousands_sep = ',' if last_char_sep else '.'
            return decimal_sep, thousands_sep
        
        # Only one type
        if comma_count > 1:  # Multiple commas likely thousands
            return '.', ','
        elif dot_count > 1:  # Multiple dots likely thousands
            return ',', '.'
        
        # Default to period as decimal
        return '.', None
    
    @staticmethod
    def normalize_email(email: str) -> Optional[str]:
        """Normalize email address."""
        if not email or not isinstance(email, str):
            return None
        
        email = email.strip().lower()
        
        # Basic email validation
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return email
        
        return None
    
    @staticmethod
    def normalize_phone(phone: str) -> Optional[str]:
        """Normalize phone number (remove special chars)."""
        if not phone or not isinstance(phone, str):
            return None
        
        # Remove common phone formatting
        phone = re.sub(r'[^0-9+]', '', phone)
        
        return phone if phone else None
    
    @staticmethod
    def normalize_boolean(value: Any) -> Optional[bool]:
        """Normalize to boolean."""
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            value = value.strip().lower()
            if value in ('true', 'yes', '1', 'y', 'on'):
                return True
            elif value in ('false', 'no', '0', 'n', 'off'):
                return False
        
        elif isinstance(value, (int, float)):
            return bool(value)
        
        return None
    
    @staticmethod
    def normalize_text(text: str, max_length: Optional[int] = None) -> Optional[str]:
        """Normalize text (trim, deduplicate spaces)."""
        if not text or not isinstance(text, str):
            return None
        
        text = text.strip()
        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text)
        
        if max_length:
            text = text[:max_length]
        
        return text if text else None
