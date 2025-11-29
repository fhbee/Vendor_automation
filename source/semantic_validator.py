"""
Semantic validation engine.
Validates business logic and domain-specific rules.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class SemanticRule:
    """Semantic validation rule."""
    
    def __init__(self, rule_type: str, **kwargs):
        self.rule_type = rule_type
        self.kwargs = kwargs
        self.message = kwargs.get('message', 'Semantic validation failed')
    
    def validate(self, canonical_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Execute semantic validation."""
        if self.rule_type == 'status_transition':
            return self._validate_status_transition(canonical_data)
        elif self.rule_type == 'business_hours':
            return self._validate_business_hours(canonical_data)
        elif self.rule_type == 'inventory':
            return self._validate_inventory(canonical_data)
        
        return None
    
    def _validate_status_transition(self, canonical_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Validate valid status transitions."""
        current_status = canonical_data.get('order_status')
        allowed_transitions = self.kwargs.get('allowed_from', [])
        
        if current_status not in allowed_transitions:
            return {
                'field': 'order_status',
                'rule': 'status_transition',
                'message': self.message
            }
        
        return None
    
    def _validate_business_hours(self, canonical_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Validate order created during business hours."""
        from datetime import datetime
        
        created_at = canonical_data.get('created_at')
        if created_at:
            try:
                dt = datetime.fromisoformat(str(created_at))
                hour = dt.hour
                
                # Assume business hours 9-17
                if hour < 9 or hour >= 17:
                    return {
                        'field': 'created_at',
                        'rule': 'business_hours',
                        'message': self.message
                    }
            except ValueError:
                pass
        
        return None
    
    def _validate_inventory(self, canonical_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Validate inventory checks (custom logic)."""
        # Placeholder for domain-specific inventory validation
        return None


class SemanticValidator:
    """Validates semantic/business logic."""
    
    def __init__(self, rules: List[SemanticRule]):
        self.rules = rules
    
    def validate(self, canonical_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Validate all semantic rules. Returns list of errors."""
        errors = []
        
        for rule in self.rules:
            error = rule.validate(canonical_data)
            if error:
                errors.append(error)
        
        return errors
