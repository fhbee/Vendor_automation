"""
Utility functions for hashing, path safety, and data transformations.
"""

import hashlib
import logging
from pathlib import Path
from typing import Any
import uuid
from datetime import datetime


logger = logging.getLogger(__name__)


def compute_file_hash(file_path: Path, algorithm: str = 'sha256') -> str:
    """
    Compute hash of a file for deterministic file identification.
    
    Args:
        file_path: Path to file
        algorithm: Hash algorithm ('sha256', 'md5', etc.)
    
    Returns:
        Hex-encoded hash string
    """
    hash_func = hashlib.new(algorithm)
    chunk_size = 8192
    
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()


def compute_bytes_hash(data: bytes, algorithm: str = 'sha256') -> str:
    """Compute hash of bytes."""
    hash_func = hashlib.new(algorithm)
    hash_func.update(data)
    return hash_func.hexdigest()


def generate_row_id(file_id: str, line_number: int) -> str:
    """Generate deterministic row ID from file_id and line number."""
    return f"{file_id}_{line_number}"


def generate_batch_id() -> str:
    """Generate unique batch ID with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = str(uuid.uuid4())[:8]
    return f"batch_{timestamp}_{unique_suffix}"


def sanitize_path(path: str, base_dir: Path = None) -> Path:
    """
    Sanitize a file path to prevent path traversal attacks.
    
    Args:
        path: Path string to sanitize
        base_dir: Base directory for relative path validation
    
    Returns:
        Sanitized Path object
    
    Raises:
        ValueError: If path is unsafe
    """
    safe_path = Path(path).resolve()
    
    if base_dir:
        base_dir = Path(base_dir).resolve()
        try:
            safe_path.relative_to(base_dir)
        except ValueError:
            raise ValueError(f"Path {path} escapes base directory {base_dir}")
    
    return safe_path


def archive_raw_file(source_path: Path, archive_dir: Path, file_id: str) -> Path:
    """
    Archive raw file to preserve original data.
    
    Args:
        source_path: Original file path
        archive_dir: Directory to store archive
        file_id: File hash ID
    
    Returns:
        Path to archived file
    """
    archive_dir = Path(archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # Use file_id + original extension
    ext = source_path.suffix
    archive_path = archive_dir / f"{file_id}{ext}"
    
    # Copy file preserving metadata
    import shutil
    shutil.copy2(source_path, archive_path)
    logger.info(f"Archived raw file: {archive_path}")
    
    return archive_path


def ensure_directory(dir_path: Path) -> Path:
    """Ensure directory exists, create if not."""
    dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def safe_read_file(file_path: Path, encoding: str = 'utf-8') -> str:
    """Safely read file with error handling."""
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            return f.read()
    except UnicodeDecodeError:
        logger.warning(f"Failed to read {file_path} as UTF-8, trying latin-1")
        with open(file_path, 'r', encoding='latin-1') as f:
            return f.read()


def safe_write_file(file_path: Path, content: str, encoding: str = 'utf-8') -> None:
    """Safely write to file with directory creation."""
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(file_path, 'w', encoding=encoding) as f:
        f.write(content)
    logger.debug(f"Written to file: {file_path}")


def merge_dicts(base: dict, override: dict) -> dict:
    """Deep merge override dict into base dict."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def truncate_string(s: str, max_length: int = 100) -> str:
    """Truncate string for logging/display."""
    if len(s) <= max_length:
        return s
    return s[:max_length - 3] + "..."
