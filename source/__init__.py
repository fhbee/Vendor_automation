"""
Vendor automation package.
"""

__version__ = "0.1.0"
__author__ = "Automation Team"

from .models import (
    FileMeta,
    Row,
    BatchResult,
    RowStatus,
    FileStatus,
    CanonicalSchema,
    MappingRule,
    ValidationRuleSet,
)
from .config_loader import ConfigLoader
from .logging_setup import setup_logging, get_logger
from .metadata_store import MetadataStore
from .utils import (
    compute_file_hash,
    generate_row_id,
    generate_batch_id,
    sanitize_path,
)

__all__ = [
    'FileMeta',
    'Row',
    'BatchResult',
    'RowStatus',
    'FileStatus',
    'CanonicalSchema',
    'MappingRule',
    'ValidationRuleSet',
    'ConfigLoader',
    'setup_logging',
    'get_logger',
    'MetadataStore',
    'compute_file_hash',
    'generate_row_id',
    'generate_batch_id',
    'sanitize_path',
]
