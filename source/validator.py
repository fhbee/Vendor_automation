"""
Field-level and row-level validation engine.
Applies structural and business rules to canonical data.
"""

import logging
import re
from typing import Any, Dict, List, Optional
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger(__name__)


class ValidationRule:
    """Single validation rule with execution logic."""
    
    VALIDATORS = {}
    
    def __init__(self, field: str, rule_type: str, **kwargs):
        self.field = field
        self.rule_type = rule_type
        self.kwargs = kwargs
        self.message = kwargs.get('message', f'{field} failed {rule_type}')
    
    def validate(self, value: Any) -> tuple:
        """Execute validation. Returns (is_valid, error_message)."""
        validator_func = ValidationRule.VALIDATORS.get(self.rule_type)
        if not validator_func:
            logger.warning(f'Unknown rule type: {self.rule_type}')
            return True, None
        
        return validator_func(value, self.message, self.kwargs)


# Validator functions
def _required(value: Any, message: str, kwargs: dict) -> tuple:
    """Check if value is required (not None/empty)."""
    if value is None or (isinstance(value, str) and value.strip() == ''):
        return False, message
    return True, None


def _type_check(value: Any, message: str, kwargs: dict) -> tuple:
    """Check value type."""
    expected_type = kwargs.get('expected_type', str)
    try:
        if expected_type == 'integer':
            int(value)
        elif expected_type == 'decimal':
            Decimal(value)
        elif expected_type == 'date':
            datetime.fromisoformat(str(value))
        elif expected_type == 'email':
            if not re.match(r'^[^@]+@[^@]+\.[^@]+$', str(value)):
                return False, message
        return True, None
    except (ValueError, TypeError):
        return False, message


def _range_check(value: Any, message: str, kwargs: dict) -> tuple:
    """Check if value is within range."""
    try:
        val = Decimal(value) if value is not None else Decimal('0')
        min_val = kwargs.get('min_value')
        max_val = kwargs.get('max_value')
        
        if min_val is not None and val < Decimal(str(min_val)):
            return False, message
        if max_val is not None and val > Decimal(str(max_val)):
            return False, message
        return True, None
    except (ValueError, TypeError):
        return False, message


def _enum_check(value: Any, message: str, kwargs: dict) -> tuple:
    """Check if value is in enum."""
    allowed = kwargs.get('values', [])
    if str(value).upper() not in [str(v).upper() for v in allowed]:
        return False, message
    return True, None


def _regex_check(value: Any, message: str, kwargs: dict) -> tuple:
    """Check if value matches regex."""
    pattern = kwargs.get('pattern')
    if pattern and not re.match(pattern, str(value)):
        return False, message
    return True, None


def _length_check(value: Any, message: str, kwargs: dict) -> tuple:
    """Check string length."""
    val_str = str(value) if value is not None else ''
    min_len = kwargs.get('min_length', 0)
    max_len = kwargs.get('max_length', float('inf'))
    
    if len(val_str) < min_len or len(val_str) > max_len:
        return False, message
    return True, None


def _date_format_check(value: Any, message: str, kwargs: dict) -> tuple:
    """Check ISO8601 date format."""
    if value is None:
        return True, None
    try:
        # Expect YYYY-MM-DD
        datetime.fromisoformat(str(value))
        return True, None
    except ValueError:
        return False, message


# Register validators
ValidationRule.VALIDATORS = {
    'required': _required,
    'type': _type_check,
    'range': _range_check,
    'enum': _enum_check,
    'regex': _regex_check,
    'length': _length_check,
    'date_format': _date_format_check,
}


class RowValidator:
    """Validates single row against rules."""
    
    def __init__(self, rules: List[ValidationRule]):
        self.rules = rules
    
    def validate(self, canonical_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Validate row. Returns list of errors.
        
        Args:
            canonical_data: Canonical data dict
        
        Returns:
            List of {'field': ..., 'rule': ..., 'message': ...}
        """
        errors = []
        
        for rule in self.rules:
            if rule.field not in canonical_data:
                continue
            
            value = canonical_data[rule.field]
            is_valid, error_msg = rule.validate(value)
            
            if not is_valid:
                errors.append({
                    'field': rule.field,
                    'rule': rule.rule_type,
                    'message': error_msg or rule.message
                })
        
        return errors
