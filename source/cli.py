"""
CLI interface for user interactions.
Provides commands for viewing, approving, and managing flagged rows.
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional
from tabulate import tabulate

from .metadata_store import MetadataStore
from .models import RowStatus

logger = logging.getLogger(__name__)


class CLI:
    """Command-line interface for pipeline management."""
    
    def __init__(self, db_path: Path):
        """
        Initialize CLI.
        
        Args:
            db_path: Path to SQLite metadata database
        """
        self.metadata_store = MetadataStore(db_path)
    
    def view_flagged(self, batch_id: str, limit: int = 100, offset: int = 0) -> None:
        """
        View flagged rows for batch.
        
        Args:
            batch_id: Batch ID
            limit: Number of rows to display
            offset: Offset for pagination
        """
        flagged_rows = self.metadata_store.query_flagged(batch_id, limit, offset)
        
        if not flagged_rows:
            print(f"No flagged rows for batch {batch_id}")
            return
        
        # Prepare table data
        table_data = []
        for row in flagged_rows:
            table_data.append([
                row.row_id[:8],  # Shortened ID
                row.line_number,
                ', '.join([e.get('field', '?') for e in row.validation_errors[:2]]),  # Top 2 errors
                len(row.validation_errors),
                row.status.value
            ])
        
        print(f"\nFlagged rows for batch {batch_id} (showing {len(flagged_rows)}):\n")
        print(tabulate(table_data, 
                      headers=['Row ID', 'Line', 'Errors', 'Error Count', 'Status'],
                      tablefmt='grid'))
    
    def view_row_details(self, row_id: str) -> None:
        """
        View detailed information for a row.
        
        Args:
            row_id: Row ID
        """
        row = self.metadata_store.query_row(row_id)
        
        if not row:
            print(f"Row {row_id} not found")
            return
        
        print(f"\n{'='*60}")
        print(f"Row ID: {row.row_id}")
        print(f"File ID: {row.file_id}")
        print(f"Line Number: {row.line_number}")
        print(f"Status: {row.status.value}")
        
        print(f"\nRaw Data:")
        for key, value in (row.raw_data or {}).items():
            print(f"  {key}: {value}")
        
        print(f"\nCanonical Data:")
        for key, value in (row.canonical_data or {}).items():
            print(f"  {key}: {value}")
        
        if row.validation_errors:
            print(f"\nValidation Errors ({len(row.validation_errors)}):")
            for error in row.validation_errors:
                print(f"  - {error.get('field', '?')}: {error.get('message', '?')}")
        
        print(f"{'='*60}\n")
    
    def approve_rows(self, row_ids: List[str], reviewer_id: str, 
                    comment: str = None) -> None:
        """
        Approve flagged rows.
        
        Args:
            row_ids: List of row IDs
            reviewer_id: Reviewer identifier
            comment: Optional review comment
        """
        for row_id in row_ids:
            self.metadata_store.set_reviewer_decision(
                row_id=row_id,
                decision='approved',
                reviewer_id=reviewer_id,
                comment=comment or ''
            )
            logger.info(f"Approved row {row_id}")
        
        print(f"✓ Approved {len(row_ids)} rows")
    
    def reject_rows(self, row_ids: List[str], reviewer_id: str,
                   comment: str = None) -> None:
        """
        Reject flagged rows.
        
        Args:
            row_ids: List of row IDs
            reviewer_id: Reviewer identifier
            comment: Optional review comment
        """
        for row_id in row_ids:
            self.metadata_store.set_reviewer_decision(
                row_id=row_id,
                decision='rejected',
                reviewer_id=reviewer_id,
                comment=comment or ''
            )
            logger.info(f"Rejected row {row_id}")
        
        print(f"✓ Rejected {len(row_ids)} rows")
    
    def view_batch_summary(self, batch_id: str) -> None:
        """
        View batch summary statistics.
        
        Args:
            batch_id: Batch ID
        """
        files = self.metadata_store.get_files_by_batch(batch_id)
        
        if not files:
            print(f"Batch {batch_id} not found")
            return
        
        total_rows = 0
        valid_rows = 0
        flagged_rows = 0
        
        for file_meta in files:
            rows = self.metadata_store.get_rows_by_file(file_meta.file_id)
            total_rows += len(rows)
            valid_rows += len([r for r in rows if r.status == RowStatus.VALID])
            flagged_rows += len([r for r in rows if r.status == RowStatus.FLAGGED])
        
        print(f"\n{'='*60}")
        print(f"Batch: {batch_id}")
        print(f"Total Rows: {total_rows}")
        print(f"Valid Rows: {valid_rows} ({100*valid_rows//max(1,total_rows)}%)")
        print(f"Flagged Rows: {flagged_rows} ({100*flagged_rows//max(1,total_rows)}%)")
        print(f"Files: {len(files)}")
        
        for file_meta in files:
            rows = self.metadata_store.get_rows_by_file(file_meta.file_id)
            file_valid = len([r for r in rows if r.status == RowStatus.VALID])
            file_flagged = len([r for r in rows if r.status == RowStatus.FLAGGED])
            print(f"  - {file_meta.filename}: {len(rows)} rows ({file_valid} valid, {file_flagged} flagged)")
        
        print(f"{'='*60}\n")
    
    def export_flagged_csv(self, batch_id: str, output_path: Path) -> None:
        """
        Export flagged rows to CSV for review.
        
        Args:
            batch_id: Batch ID
            output_path: Path to export CSV
        """
        import csv
        
        flagged_rows = self.metadata_store.query_flagged(batch_id, limit=10000)
        
        if not flagged_rows:
            print(f"No flagged rows for batch {batch_id}")
            return
        
        output_path = Path(output_path)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow(['row_id', 'line_number', 'error_field', 'error_rule', 'error_message', 'canonical_data'])
            
            # Rows
            for row in flagged_rows:
                for error in row.validation_errors:
                    writer.writerow([
                        row.row_id,
                        row.line_number,
                        error.get('field', ''),
                        error.get('rule', ''),
                        error.get('message', ''),
                        str(row.canonical_data)
                    ])
        
        print(f"✓ Exported {len(flagged_rows)} flagged rows to {output_path}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Pipeline CLI for batch management")
    
    parser.add_argument('--db', type=Path, default=Path('./metadata/auto.db'), 
                       help='Database path')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # view-flagged command
    view_parser = subparsers.add_parser('view-flagged', help='View flagged rows')
    view_parser.add_argument('--batch', type=str, required=True, help='Batch ID')
    view_parser.add_argument('--limit', type=int, default=100, help='Number of rows')
    
    # view-row command
    row_parser = subparsers.add_parser('view-row', help='View row details')
    row_parser.add_argument('--row-id', type=str, required=True, help='Row ID')
    
    # approve-rows command
    approve_parser = subparsers.add_parser('approve-rows', help='Approve flagged rows')
    approve_parser.add_argument('--row-ids', type=str, required=True, help='Comma-separated row IDs')
    approve_parser.add_argument('--reviewer', type=str, required=True, help='Reviewer ID')
    approve_parser.add_argument('--comment', type=str, help='Review comment')
    
    # reject-rows command
    reject_parser = subparsers.add_parser('reject-rows', help='Reject flagged rows')
    reject_parser.add_argument('--row-ids', type=str, required=True, help='Comma-separated row IDs')
    reject_parser.add_argument('--reviewer', type=str, required=True, help='Reviewer ID')
    reject_parser.add_argument('--comment', type=str, help='Review comment')
    
    # batch-summary command
    summary_parser = subparsers.add_parser('batch-summary', help='View batch summary')
    summary_parser.add_argument('--batch', type=str, required=True, help='Batch ID')
    
    # export-flagged command
    export_parser = subparsers.add_parser('export-flagged', help='Export flagged rows to CSV')
    export_parser.add_argument('--batch', type=str, required=True, help='Batch ID')
    export_parser.add_argument('--output', type=Path, required=True, help='Output CSV path')
    
    args = parser.parse_args()
    
    cli = CLI(args.db)
    
    if args.command == 'view-flagged':
        cli.view_flagged(args.batch, limit=args.limit)
    elif args.command == 'view-row':
        cli.view_row_details(args.row_id)
    elif args.command == 'approve-rows':
        row_ids = args.row_ids.split(',')
        cli.approve_rows(row_ids, args.reviewer, args.comment)
    elif args.command == 'reject-rows':
        row_ids = args.row_ids.split(',')
        cli.reject_rows(row_ids, args.reviewer, args.comment)
    elif args.command == 'batch-summary':
        cli.view_batch_summary(args.batch)
    elif args.command == 'export-flagged':
        cli.export_flagged_csv(args.batch, args.output)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
