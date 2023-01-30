"""Database operations for storing historical metrics."""

import sqlite3
from datetime import datetime
from dataclasses import asdict
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

class MetricsDB:
    """Handles storage and retrieval of historical metrics."""
    
    def __init__(self, db_path: str = "metrics.sqlite"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS table_metrics (
                    table_name TEXT,
                    column_name TEXT,
                    pattern_type TEXT,
                    avg_hourly_updates REAL,
                    stddev_updates REAL,
                    active_hours INTEGER,
                    inactive_hours INTEGER,
                    last_updated TIMESTAMP,
                    PRIMARY KEY (table_name, column_name)
                )
            """)
            conn.commit()

    def store_metrics(self, table_name: str, column_name: str, pattern_type: str, metrics: Dict):
        """Store metrics for a table's timestamp column."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO table_metrics 
                (table_name, column_name, pattern_type, avg_hourly_updates, 
                 stddev_updates, active_hours, inactive_hours, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                table_name, column_name, pattern_type,
                metrics['avg_hourly_updates'], metrics['stddev_updates'],
                metrics['active_hours'], metrics['inactive_hours'],
                datetime.now().isoformat()
            ))
            conn.commit()

    def get_metrics(self, table_name: str, column_name: str) -> Optional[Dict]:
        """Retrieve stored metrics for a table's timestamp column."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM table_metrics 
                WHERE table_name = ? AND column_name = ?
            """, (table_name, column_name))
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            return None

    def check_staleness(self, table_name: str, column_name: str, current_delay: float) -> bool:
        """Check if current delay exceeds expected patterns."""
        metrics = self.get_metrics(table_name, column_name)
        if not metrics:
            return False

        # Define staleness thresholds based on pattern type
        thresholds = {
            'realtime': 15,  # 15 minutes
            'hourly_batch': 60,  # 1 hour
            'daily_batch': 1440,  # 24 hours
            'irregular': 10080,  # 7 days
        }

        threshold = thresholds.get(metrics['pattern_type'], 1440)  # default to 24 hours
        
        if current_delay > threshold:
            logger.warning(
                f"Data staleness detected for {table_name}.{column_name}. "
                f"Current delay: {current_delay} minutes. "
                f"Expected threshold for {metrics['pattern_type']}: {threshold} minutes"
            )
            return True
        return False
