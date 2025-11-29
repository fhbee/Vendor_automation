"""
Cross-field validation engine.
Validates relationships between multiple fields (formulas, dependencies).
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class CrossFieldRule:
    """Cross-field validation rule."""
    
    def __init__(self, fields: List[str], rule_type: str, **kwargs):
        self.fields = fields
        self.rule_type = rule_type
        self.kwargs = kwargs
        self.message = kwargs.get('message', 'Cross-field rule failed')
    
    def validate(self, canonical_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Execute validation. Returns error dict or None."""
        if self.rule_type == 'formula':
            return self._validate_formula(canonical_data)
        elif self.rule_type == 'dependency':
            return self._validate_dependency(canonical_data)
        elif self.rule_type == 'mutual_exclusion':
            return self._validate_mutual_exclusion(canonical_data)
        
        return None
    
    def _validate_formula(self, canonical_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Validate mathematical formula (e.g., qty * price == total)."""
        formula = self.kwargs.get('formula', '')
        
        try:
            # Extract field values
            field_values = {}
            for field in self.fields:
                val = canonical_data.get(field)
                if val is not None:
                    try:
                        field_values[field] = float(val)
                    except (ValueError, TypeError):
                        field_values[field] = val
            
            # Evaluate formula
            result = eval(formula, {"__builtins__": {}}, field_values)
            
            if not result:
                return {
                    'fields': ','.join(self.fields),
                    'rule': 'formula',
                    'message': self.message
                }
        except Exception as e:
            logger.warning(f'Formula evaluation error: {e}')
        
        return None
    
    def _validate_dependency(self, canonical_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Validate field dependency (if A then B required)."""
        condition_field = self.kwargs.get('condition_field')
        condition_value = self.kwargs.get('condition_value')
        required_field = self.kwargs.get('required_field')
        
        if canonical_data.get(condition_field) == condition_value:
            if not canonical_data.get(required_field):
                return {
                    'fields': f'{condition_field},{required_field}',
                    'rule': 'dependency',
                    'message': self.message
                }
        
        return None
    
    def _validate_mutual_exclusion(self, canonical_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Validate mutual exclusion (only one of fields can be set)."""
        set_fields = [f for f in self.fields if canonical_data.get(f)]
        
        if len(set_fields) > 1:
            return {
                'fields': ','.join(self.fields),
                'rule': 'mutual_exclusion',
                'message': self.message
            }
        
        return None


class CrossFieldValidator:
    """Validates cross-field rules."""
    
    def __init__(self, rules: List[CrossFieldRule]):
        self.rules = rules
    
    def validate(self, canonical_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Validate all cross-field rules. Returns list of errors."""
        errors = []
        
        for rule in self.rules:
            error = rule.validate(canonical_data)
            if error:
                errors.append(error)
        
        return errors
