"""
AI Suggester module for intelligent field mapping.
Uses embeddings or LLM-based approach for header mapping suggestions.
Implements caching and controlled confidence thresholds.
"""

import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)


@dataclass
class Suggestion:
    """Mapping suggestion."""
    canonical_field: str
    candidate_header: str
    confidence: float
    rationale: str


class AISuggester:
    """Generates intelligent mapping suggestions with controlled confidence."""
    
    def __init__(self, ai_config: Dict[str, Any], metadata_store=None):
        """
        Initialize suggester.
        
        Args:
            ai_config: AI configuration (provider, API key, thresholds)
            metadata_store: Optional metadata store for caching
        """
        self.ai_config = ai_config
        self.metadata_store = metadata_store
        self.provider = ai_config.get('provider', 'local')
        self.api_key = ai_config.get('api_key')
    
    def suggest_headers(self, vendor_headers: List[str], 
                       canonical_fields: List[str],
                       sample_rows: List[Dict[str, Any]]) -> List[Suggestion]:
        """
        Suggest mappings for vendor headers to canonical fields.
        
        Args:
            vendor_headers: List of vendor column headers
            canonical_fields: List of canonical field names
            sample_rows: Sample data rows for context
        
        Returns:
            List of mapping suggestions
        """
        logger.info(f"Suggesting mappings for {len(vendor_headers)} headers to {len(canonical_fields)} fields")
        
        # Check cache first
        if self.metadata_store:
            cached = self._get_cached_suggestions(vendor_headers)
            if cached:
                logger.debug("Using cached suggestions")
                return cached
        
        # Deterministic fallback: exact matching first
        suggestions = self._deterministic_match(vendor_headers, canonical_fields)
        
        # If deterministic match not sufficient, use AI
        unmapped_headers = [h for h in vendor_headers if not self._is_mapped(h, suggestions)]
        if unmapped_headers and self.provider != 'local':
            ai_suggestions = self._ai_suggest(unmapped_headers, canonical_fields, sample_rows)
            suggestions.extend(ai_suggestions)
        
        # Cache results
        if self.metadata_store:
            self._cache_suggestions(vendor_headers, suggestions)
        
        return suggestions
    
    def _deterministic_match(self, headers: List[str], 
                            fields: List[str]) -> List[Suggestion]:
        """Apply deterministic matching rules."""
        suggestions = []
        
        for header in headers:
            # Exact match (case-insensitive)
            for field in fields:
                if header.lower() == field.lower():
                    suggestions.append(Suggestion(
                        canonical_field=field,
                        candidate_header=header,
                        confidence=1.0,
                        rationale="Exact match"
                    ))
                    break
            
            # Normalized match (remove punctuation, tokenize)
            for field in fields:
                if self._normalize_string(header) == self._normalize_string(field):
                    suggestions.append(Suggestion(
                        canonical_field=field,
                        candidate_header=header,
                        confidence=0.95,
                        rationale="Normalized match"
                    ))
                    break
        
        return suggestions
    
    def _normalize_string(self, s: str) -> str:
        """Normalize string for matching."""
        import re
        # Remove punctuation, convert to lowercase
        normalized = re.sub(r'[^\w\s]', '', s).lower()
        return normalized
    
    def _ai_suggest(self, headers: List[str], fields: List[str],
                   sample_rows: List[Dict[str, Any]]) -> List[Suggestion]:
        """Call AI API for suggestions."""
        try:
            if self.provider == 'openai':
                return self._suggest_openai(headers, fields, sample_rows)
            else:
                logger.warning(f"Unknown AI provider: {self.provider}")
                return []
        except Exception as e:
            logger.error(f"Error calling AI suggester: {e}")
            return []
    
    def _suggest_openai(self, headers: List[str], fields: List[str],
                       sample_rows: List[Dict[str, Any]]) -> List[Suggestion]:
        """Call OpenAI API for suggestions."""
        try:
            import openai
            
            # Build prompt
            sample_data = json.dumps(sample_rows[:3], indent=2)  # Limit samples
            prompt = f"""You are a data mapping assistant. Given vendor column headers and canonical field names, suggest the best mapping.

Vendor Headers: {', '.join(headers)}

Canonical Fields: {', '.join(fields)}

Sample Data (first 3 rows):
{sample_data}

Return a JSON array with objects containing:
- canonical_field: The canonical field name
- vendor_header: The vendor header to map
- confidence: Confidence score 0-1 (1.0 = certain, 0.0 = uncertain)
- rationale: Brief explanation

Rules:
1. Only map to canonical fields from the provided list
2. Do NOT invent fields
3. Prefer exact or substring matches
4. Set confidence to 0.95 if uncertain
5. Include all unmapped headers in response

Return ONLY valid JSON array, no other text."""
            
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
                temperature=0.3,
                timeout=10
            )
            
            # Parse response
            response_text = response.choices[0].message.content
            suggestions_data = json.loads(response_text)
            
            suggestions = []
            for item in suggestions_data:
                suggestions.append(Suggestion(
                    canonical_field=item['canonical_field'],
                    candidate_header=item['vendor_header'],
                    confidence=float(item['confidence']),
                    rationale=item['rationale']
                ))
            
            logger.info(f"AI suggester returned {len(suggestions)} suggestions")
            return suggestions
        
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return []
    
    def _get_cached_suggestions(self, headers: List[str]) -> Optional[List[Suggestion]]:
        """Get cached suggestions for headers."""
        if not self.metadata_store:
            return None
        
        # Generate cache key from headers
        header_sig = hash(tuple(sorted(headers)))
        
        # Query metadata store
        try:
            cached = self.metadata_store.get_cached_suggestions(header_sig)
            if cached:
                return json.loads(cached)
        except:
            pass
        
        return None
    
    def _cache_suggestions(self, headers: List[str], suggestions: List[Suggestion]) -> None:
        """Cache suggestions."""
        if not self.metadata_store:
            return
        
        try:
            header_sig = hash(tuple(sorted(headers)))
            suggestions_json = json.dumps([
                {
                    'canonical_field': s.canonical_field,
                    'candidate_header': s.candidate_header,
                    'confidence': s.confidence,
                    'rationale': s.rationale
                }
                for s in suggestions
            ])
            self.metadata_store.store_cached_suggestions(header_sig, suggestions_json)
        except Exception as e:
            logger.warning(f"Error caching suggestions: {e}")
    
    def _is_mapped(self, header: str, suggestions: List[Suggestion]) -> bool:
        """Check if header is already mapped."""
        return any(s.candidate_header == header for s in suggestions)
