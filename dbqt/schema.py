"""Schema discovery and analysis functionality."""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
import sqeleton
from sqeleton.queries.api import table, this

logger = logging.getLogger(__name__)

@dataclass
class ColumnMetadata:
    """Metadata for a database column."""
    name: str
    data_type: str
    is_nullable: bool
    sample_values: List[str]

@dataclass 
class TableMetadata:
    """Metadata for a database table."""
    name: str
    columns: List[ColumnMetadata]
    update_metrics: Optional[Dict] = None
    pattern_type: Optional[str] = None

class SchemaDiscovery:
    """Discovers and analyzes database schema."""
    
    def __init__(self, db_connection):
        """Initialize with a sqeleton database connection."""
        self.db = db_connection

    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """Get metadata for the specified table."""
        # Query schema info using sqeleton
        schema = self.db.query_table_schema((table_name,))
        
        columns = []
        for col_name, col_info in schema.items():
            # Sample data for the column using sqeleton query
            sample_query = (
                table(table_name)
                .select(this[col_name])
                .limit(100)
            )
            # Execute query and extract first value from each row tuple
            samples = [str(row[0]) for row in self.db.query(sample_query)]
            
            # Extract data type safely
            data_type = col_info[2] if len(col_info) > 2 else None
            if data_type is None:
                logger.warning(f"No data type information for column {col_name} in table {table_name}")
                data_type = "unknown"
                
            col_meta = ColumnMetadata(
                name=col_name,
                data_type=data_type,
                is_nullable=True,  # TODO: Get from schema when available
                sample_values=samples
            )
            columns.append(col_meta)

        return TableMetadata(
            name=table_name,
            columns=columns
        )

    def discover_timestamp_columns(self, table_meta: TableMetadata) -> List[str]:
        """Identify timestamp columns in the table."""
        timestamp_cols = []
        
        for col in table_meta.columns:
            # Check data type if available and ensure it's a string
            data_type = str(col.data_type).lower() if col.data_type is not None else ""
            if 'timestamp' in data_type or 'datetime' in data_type:
                timestamp_cols.append(col.name)
            # Check column naming patterns
            elif any(pattern in col.name.lower() for pattern in ['_at', '_date', '_time', 'timestamp']):
                # Validate sample values can be parsed as dates
                try:
                    datetime.fromisoformat(col.sample_values[0].replace(' ', 'T'))
                    timestamp_cols.append(col.name)
                except (ValueError, IndexError):
                    continue
                    
        return timestamp_cols
