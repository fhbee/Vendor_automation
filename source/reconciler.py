"""
Reconciliation and deduplication module.
Identifies likely duplicates using deterministic keys.
"""

import logging
import hashlib
from typing import Dict, List, Set, Tuple, Optional
from .models import Row

logger = logging.getLogger(__name__)


class Reconciler:
    """Identifies potential duplicates and reconciles records."""
    
    @staticmethod
    def generate_dedup_key(row: Row, key_fields: List[str]) -> str:
        """
        Generate deterministic deduplication key.
        
        Args:
            row: Row object
            key_fields: Fields to include in key
        
        Returns:
            Hash-based dedup key
        """
        canonical_data = row.canonical_data or row.raw_data
        
        key_parts = []
        for field in key_fields:
            value = canonical_data.get(field, '')
            if value is not None:
                key_parts.append(str(value).lower().strip())
        
        key_string = '|'.join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    @staticmethod
    def find_duplicates(rows: List[Row], 
                       key_fields: List[str]) -> Dict[str, List[int]]:
        """
        Find duplicate rows by key fields.
        
        Args:
            rows: List of Row objects
            key_fields: Fields to use for deduplication
        
        Returns:
            Dict mapping dedup_key to list of row indices
        """
        duplicates = {}
        
        for idx, row in enumerate(rows):
            key = Reconciler.generate_dedup_key(row, key_fields)
            
            if key not in duplicates:
                duplicates[key] = []
            duplicates[key].append(idx)
        
        # Filter to only actual duplicates
        return {k: v for k, v in duplicates.items() if len(v) > 1}
    
    @staticmethod
    def mark_duplicates(rows: List[Row], 
                       key_fields: List[str],
                       keep_first: bool = True) -> List[Row]:
        """
        Mark duplicate rows.
        
        Args:
            rows: List of Row objects
            key_fields: Fields for deduplication
            keep_first: If True, keep first occurrence
        
        Returns:
            Modified rows (duplicates flagged in validation_errors)
        """
        duplicates = Reconciler.find_duplicates(rows, key_fields)
        
        for key, indices in duplicates.items():
            if keep_first:
                dup_indices = indices[1:]  # Skip first
            else:
                dup_indices = indices
            
            for idx in dup_indices:
                rows[idx].validation_errors.append({
                    'field': '_deduplicate',
                    'rule': 'duplicate',
                    'message': f'Duplicate of row {indices[0]}'
                })
        
        logger.info(f"Found {len(duplicates)} duplicate groups")
        return rows
    
    @staticmethod
    def fuzzy_match(value1: str, value2: str, threshold: float = 0.85) -> bool:
        """
        Fuzzy string matching using simple heuristic.
        
        Args:
            value1: First value
            value2: Second value
            threshold: Match threshold (0-1)
        
        Returns:
            True if similar enough
        """
        if not value1 or not value2:
            return False
        
        v1 = value1.lower().strip()
        v2 = value2.lower().strip()
        
        if v1 == v2:
            return True
        
        # Levenshtein distance (simplified)
        len1, len2 = len(v1), len(v2)
        if abs(len1 - len2) > max(len1, len2) * 0.3:
            return False
        
        # Count common characters
        common = sum(c in v2 for c in v1)
        similarity = common / max(len1, len2)
        
        return similarity >= threshold
