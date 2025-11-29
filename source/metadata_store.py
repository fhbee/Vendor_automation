"""
Metadata store for tracking file and row processing state.
Uses SQLite for persistence and queries.
"""

import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Any
from contextlib import contextmanager
from .models import FileMeta, Row, FileStatus, RowStatus


logger = logging.getLogger(__name__)


class MetadataStore:
    """SQLite-backed metadata store for deterministic processing."""
    
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row  # Return rows as dict-like objects
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_db(self):
        """Initialize database schema on first run."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Files table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    file_id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    path TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    processed_at TEXT,
                    status TEXT NOT NULL,
                    raw_copy_path TEXT,
                    error_message TEXT,
                    row_count INTEGER DEFAULT 0,
                    valid_rows INTEGER DEFAULT 0,
                    flagged_rows INTEGER DEFAULT 0,
                    error_rows INTEGER DEFAULT 0
                )
            ''')
            
            # Rows table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rows (
                    row_id TEXT PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    line_number INTEGER NOT NULL,
                    raw_data TEXT NOT NULL,
                    normalized_data TEXT,
                    canonical_data TEXT,
                    status TEXT NOT NULL,
                    validation_errors TEXT,
                    mapping_confidence TEXT,
                    ai_suggestions TEXT,
                    approved_by TEXT,
                    approved_at TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (file_id) REFERENCES files(file_id)
                )
            ''')
            
            # Batches table (for tracking processing runs)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    total_rows INTEGER DEFAULT 0,
                    valid_rows INTEGER DEFAULT 0,
                    flagged_rows INTEGER DEFAULT 0,
                    error_rows INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    errors TEXT
                )
            ''')
            
            # Approval history (for audit trail)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS approvals (
                    approval_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    row_id TEXT NOT NULL,
                    approved_by TEXT NOT NULL,
                    approved_at TEXT NOT NULL,
                    notes TEXT,
                    FOREIGN KEY (row_id) REFERENCES rows(row_id)
                )
            ''')
            
            # Create indices for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_created ON files(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rows_file_id ON rows(file_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rows_status ON rows(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_rows_created ON rows(created_at)')
            
            conn.commit()
            logger.info(f"Metadata store initialized: {self.db_path}")
    
    # File operations
    
    def insert_file(self, file_meta: FileMeta) -> None:
        """Insert or update file metadata."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO files (
                    file_id, filename, path, file_type, size_bytes,
                    created_at, processed_at, status, raw_copy_path,
                    error_message, row_count, valid_rows, flagged_rows, error_rows
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_meta.file_id,
                file_meta.filename,
                file_meta.path,
                file_meta.file_type,
                file_meta.size_bytes,
                file_meta.created_at.isoformat(),
                file_meta.processed_at.isoformat() if file_meta.processed_at else None,
                file_meta.status.value,
                file_meta.raw_copy_path,
                file_meta.error_message,
                file_meta.row_count,
                file_meta.valid_rows,
                file_meta.flagged_rows,
                file_meta.error_rows,
            ))
            conn.commit()
            logger.debug(f"Inserted file: {file_meta.file_id}")
    
    def get_file(self, file_id: str) -> Optional[FileMeta]:
        """Retrieve file metadata by ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM files WHERE file_id = ?', (file_id,))
            row = cursor.fetchone()
            
            if row:
                return self._row_to_file_meta(dict(row))
            return None
    
    def file_exists(self, file_id: str) -> bool:
        """Check if a file has been processed."""
        return self.get_file(file_id) is not None
    
    def get_files_by_status(self, status: FileStatus) -> list[FileMeta]:
        """Get all files with a given status."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM files WHERE status = ?', (status.value,))
            rows = cursor.fetchall()
            return [self._row_to_file_meta(dict(r)) for r in rows]
    
    def _row_to_file_meta(self, row: dict) -> FileMeta:
        """Convert database row to FileMeta object."""
        return FileMeta(
            file_id=row['file_id'],
            filename=row['filename'],
            path=row['path'],
            file_type=row['file_type'],
            size_bytes=row['size_bytes'],
            created_at=datetime.fromisoformat(row['created_at']),
            processed_at=datetime.fromisoformat(row['processed_at']) if row['processed_at'] else None,
            status=FileStatus(row['status']),
            raw_copy_path=row['raw_copy_path'],
            error_message=row['error_message'],
            row_count=row['row_count'],
            valid_rows=row['valid_rows'],
            flagged_rows=row['flagged_rows'],
            error_rows=row['error_rows'],
        )
    
    # Row operations
    
    def insert_row(self, row: Row) -> None:
        """Insert or update row data."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO rows (
                    row_id, file_id, line_number, raw_data, normalized_data,
                    canonical_data, status, validation_errors, mapping_confidence,
                    ai_suggestions, approved_by, approved_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row.row_id,
                row.file_id,
                row.line_number,
                json.dumps(row.raw_data, default=str),
                json.dumps(row.normalized_data, default=str),
                json.dumps(row.canonical_data, default=str),
                row.status.value,
                json.dumps(row.validation_errors),
                json.dumps(row.mapping_confidence),
                json.dumps(row.ai_suggestions),
                row.approved_by,
                row.approved_at.isoformat() if row.approved_at else None,
                datetime.now().isoformat(),
            ))
            conn.commit()
    
    def get_row(self, row_id: str) -> Optional[Row]:
        """Retrieve row data by ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM rows WHERE row_id = ?', (row_id,))
            db_row = cursor.fetchone()
            
            if db_row:
                return self._db_row_to_row(dict(db_row))
            return None
    
    def get_rows_by_file(self, file_id: str) -> list[Row]:
        """Get all rows for a file."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM rows WHERE file_id = ? ORDER BY line_number', (file_id,))
            db_rows = cursor.fetchall()
            return [self._db_row_to_row(dict(r)) for r in db_rows]
    
    def get_rows_by_status(self, status: RowStatus, limit: Optional[int] = None) -> list[Row]:
        """Get rows with a given status."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = 'SELECT * FROM rows WHERE status = ? ORDER BY created_at DESC'
            if limit:
                query += f' LIMIT {limit}'
            cursor.execute(query, (status.value,))
            db_rows = cursor.fetchall()
            return [self._db_row_to_row(dict(r)) for r in db_rows]
    
    def _db_row_to_row(self, db_row: dict) -> Row:
        """Convert database row to Row object."""
        return Row(
            row_id=db_row['row_id'],
            file_id=db_row['file_id'],
            line_number=db_row['line_number'],
            raw_data=json.loads(db_row['raw_data']),
            normalized_data=json.loads(db_row['normalized_data']) if db_row['normalized_data'] else {},
            canonical_data=json.loads(db_row['canonical_data']) if db_row['canonical_data'] else {},
            status=RowStatus(db_row['status']),
            validation_errors=json.loads(db_row['validation_errors']) if db_row['validation_errors'] else [],
            mapping_confidence=json.loads(db_row['mapping_confidence']) if db_row['mapping_confidence'] else {},
            ai_suggestions=json.loads(db_row['ai_suggestions']) if db_row['ai_suggestions'] else {},
            approved_by=db_row['approved_by'],
            approved_at=datetime.fromisoformat(db_row['approved_at']) if db_row['approved_at'] else None,
        )
    
    # Batch operations
    
    def insert_batch(self, batch_id: str, started_at: datetime, status: FileStatus, errors: list[str] = None) -> None:
        """Insert a batch processing record."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO batches (batch_id, started_at, status, errors)
                VALUES (?, ?, ?, ?)
            ''', (batch_id, started_at.isoformat(), status.value, json.dumps(errors or [])))
            conn.commit()
    
    def update_batch(self, batch_id: str, completed_at: datetime, status: FileStatus, totals: dict) -> None:
        """Update batch completion info."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE batches
                SET completed_at = ?, status = ?, total_rows = ?, valid_rows = ?,
                    flagged_rows = ?, error_rows = ?
                WHERE batch_id = ?
            ''', (
                completed_at.isoformat(),
                status.value,
                totals.get('total_rows', 0),
                totals.get('valid_rows', 0),
                totals.get('flagged_rows', 0),
                totals.get('error_rows', 0),
                batch_id,
            ))
            conn.commit()
    
    def get_batch(self, batch_id: str) -> Optional[dict]:
        """Retrieve batch info."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM batches WHERE batch_id = ?', (batch_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
