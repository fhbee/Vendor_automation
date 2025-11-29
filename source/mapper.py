"""
Field mapping module.
Maps vendor fields to canonical schema using deterministic rules.
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from .models import Row, MappingRule, RowStatus

logger = logging.getLogger(__name__)


class FieldMapper:
    """Maps vendor fields to canonical schema."""
    
    def __init__(self, mapping_rules: List[MappingRule]):
        self.mapping_rules = sorted(mapping_rules, key=lambda r: r.priority)
        self.unmapped_fields = set()
    
    def map_row(self, row: Row) -> Tuple[Dict[str, Any], Dict[str, float]]:
        """
        Map row fields using deterministic rules.
        
        Args:
            row: Row with raw_data and normalized_data
        
        Returns:
            Tuple of (mapped_data, confidence_scores)
        """
        mapped = {}
        confidence = {}
        
        # Get source fields
        source_data = row.normalized_data or row.raw_data
        source_fields = set(source_data.keys())
        
        # Track which vendor fields were used
        used_fields = set()
        
        # Apply each mapping rule
        for rule in self.mapping_rules:
            if rule.vendor_field not in source_fields:
                continue
            
            value = source_data[rule.vendor_field]
            canonical_field = rule.canonical_field
            
            # Apply rule
            if rule.rule_type == 'exact':
                mapped[canonical_field] = value
                confidence[canonical_field] = rule.confidence
                used_fields.add(rule.vendor_field)
            
            elif rule.rule_type == 'regex':
                if isinstance(value, str) and re.search(rule.pattern, value):
                    mapped[canonical_field] = value
                    confidence[canonical_field] = rule.confidence
                    used_fields.add(rule.vendor_field)
            
            elif rule.rule_type == 'substring':
                if isinstance(value, str) and rule.pattern in value:
                    mapped[canonical_field] = value
                    confidence[canonical_field] = rule.confidence
                    used_fields.add(rule.vendor_field)
            
            elif rule.rule_type == 'function':
                # For custom functions, skip (Phase 8)
                pass
        
        # Track unmapped fields
        unmapped = source_fields - used_fields
        for field in unmapped:
            self.unmapped_fields.add(field)
            logger.debug(f"Unmapped field: {field}")
        
        return mapped, confidence
    
    @staticmethod
    def suggest_mapping(vendor_field: str, 
                       canonical_fields: List[str]) -> List[Tuple[str, float]]:
        """
        Suggest canonical field matches (simple heuristic).
        
        Args:
            vendor_field: Vendor field name
            canonical_fields: List of available canonical fields
        
        Returns:
            List of (canonical_field, score) tuples
        """
        suggestions = []
        vendor_lower = vendor_field.lower()
        
        for canonical in canonical_fields:
            canonical_lower = canonical.lower()
            
            # Exact match
            if vendor_lower == canonical_lower:
                suggestions.append((canonical, 0.99))
            
            # Substring match
            elif canonical_lower in vendor_lower or vendor_lower in canonical_lower:
                suggestions.append((canonical, 0.7))
            
            # Levenshtein-like similarity (simple)
            else:
                score = FieldMapper._string_similarity(vendor_lower, canonical_lower)
                if score > 0.5:
                    suggestions.append((canonical, score))
        
        return sorted(suggestions, key=lambda x: x[1], reverse=True)
    
    @staticmethod
    def _string_similarity(s1: str, s2: str) -> float:
        """Calculate simple string similarity (0-1)."""
        if not s1 or not s2:
            return 0.0
        
        # Common characters
        common = len(set(s1) & set(s2))
        total = max(len(s1), len(s2))
        
        return common / total if total > 0 else 0.0


class MappingEngine:
    """Orchestrates field mapping with fallback strategies."""
    
    def __init__(self, mapper: FieldMapper):
        self.mapper = mapper
    
    def map_rows(self, rows: List[Row]) -> List[Row]:
        """
        Map multiple rows.
        
        Args:
            rows: List of Row objects
        
        Returns:
            Modified rows with canonical_data and mapping_confidence
        """
        for row in rows:
            canonical_data, confidence = self.mapper.map_row(row)
            row.canonical_data = canonical_data
            row.mapping_confidence = confidence
        
        return rows
