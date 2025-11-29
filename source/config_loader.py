"""
Configuration loading and management.
Loads YAML config files for global, vendor-specific, and canonical schema settings.
"""

import yaml
from pathlib import Path
from typing import Any, Optional
import logging
from .validator import ValidationRule
from .models import CanonicalSchema, MappingRule, ValidationRuleSet


logger = logging.getLogger(__name__)


class ConfigLoader:
    """Loads and caches configuration from YAML files."""
    
    def __init__(self, config_dir: Path):
        self.config_dir = Path(config_dir)
        self._cache = {}
    
    def _load_yaml(self, filepath: Path) -> dict[str, Any]:
        """Load a YAML file and cache it."""
        if filepath in self._cache:
            return self._cache[filepath]
        
        if not filepath.exists():
            raise FileNotFoundError(f"Config file not found: {filepath}")
        
        logger.info(f"Loading config: {filepath}")
        with open(filepath, 'r') as f:
            data = yaml.safe_load(f) or {}
        
        self._cache[filepath] = data
        return data
    
    def load_global_config(self) -> dict[str, Any]:
        """Load global configuration."""
        return self._load_yaml(self.config_dir / "global_config.yaml")
    
    def load_canonical_schema(self) -> CanonicalSchema:
        """Load and parse canonical schema definition."""
        data = self._load_yaml(self.config_dir / "canonical_schema.yaml")
        
        schema = CanonicalSchema(
            fields=data.get('fields', {}),
            version=data.get('version', '1.0')
        )
        logger.info(f"Loaded canonical schema with {len(schema.fields)} fields")
        return schema
    
    def load_vendor_config(self, vendor_name: str) -> dict[str, Any]:
        """Load vendor-specific configuration from a directory of YAML files."""
        vendor_dir = self.config_dir / "vendors" / vendor_name
        if not vendor_dir.exists() or not vendor_dir.is_dir():
            raise FileNotFoundError(f"Vendor config directory not found: {vendor_dir}")

        config = {}
        for yaml_file in vendor_dir.glob("*.yaml"):
            data = self._load_yaml(yaml_file)
            # merge each file under its basename key
            config[yaml_file.stem] = data

        logger.info(f"Loaded vendor config directory for {vendor_name} with {len(config)} files")
        return config

    
    def load_vendor_mapping_rules(self, vendor_name: str) -> list[MappingRule]:
        config = self.load_vendor_config(vendor_name)
        mapping_cfg = config.get('mapping_rules', {})

        mappings = mapping_cfg.get('mappings', {})
        priority_list = mapping_cfg.get('matching_priority', [])

        rules = []
        for vendor_field, canonical_field in mappings.items():
            rule = MappingRule(
                vendor_field=vendor_field,
                canonical_field=canonical_field,
                rule_type='exact',   # could use priority_list[0] or similar
                priority=10,
                fallback=False,
                confidence=1.0,
            )
            rules.append(rule)

        logger.info(f"Loaded {len(rules)} mapping rules for {vendor_name}")
        return rules

    
    
    def load_vendor_validation_rules(self, vendor_name: str) -> ValidationRuleSet:
        config = self.load_vendor_config(vendor_name)
        val_cfg = config.get('validation_rules', {})

        # Convert only field-level rules into ValidationRule objects
        field_rules = [
            ValidationRule(
                field=rule["field"],
                rule_type=rule["rule_type"],
                **{k: v for k, v in rule.items() if k not in ("field", "rule_type")}
            )
            for rule in val_cfg.get("field_validation_rules", [])
        ]

        # Keep cross-field and semantic rules as dicts for now
        cross_rules = val_cfg.get("cross_field_rules", [])
        semantic_rules = val_cfg.get("semantic_rules", [])

        return ValidationRuleSet(
            vendor_name=vendor_name,
            rules=field_rules,
            cross_field_rules=cross_rules,
            semantic_rules=semantic_rules,
        )

    
    def clear_cache(self):
        """Clear configuration cache (useful for testing or reload)."""
        self._cache.clear()
        logger.debug("Config cache cleared")
