"""Analysis of update patterns in timestamp columns."""

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import sqeleton
from sqeleton.queries import Count
from sqeleton.queries.api import table, this, current_timestamp
from .db import MetricsDB

logger = logging.getLogger(__name__)

@dataclass
class UpdateMetrics:
    """Metrics about column updates."""
    avg_hourly_updates: float
    stddev_updates: float 
    active_hours: int
    inactive_hours: int

class PatternAnalyzer:
    """Analyzes update patterns in timestamp columns."""

    def __init__(self, db_connection, metrics_db_path: str = "metrics.sqlite"):
        self.db = db_connection
        self.metrics_db = MetricsDB(metrics_db_path)

    def calculate_metrics(self, table_name: str, timestamp_column: str,
                        monitoring_period: int = 7) -> UpdateMetrics:
        """Calculate update metrics for the timestamp column over the monitoring period (days)."""

        t = table(table_name)

        query = (
            t.select(
                sqeleton.code("date_trunc('hour', {ts}) as hour", ts=t[timestamp_column])
            )
            .where(
                sqeleton.code("{ts} >= current_timestamp - interval '{days} days'",
                     ts=t[timestamp_column],
                     days=monitoring_period)
            )
            .group_by(sqeleton.code("hour"))
            .agg(sqeleton.code("count(*)", alias="updates"))
            .order_by(1)
        )

        hourly_counts = self.db.query(query)
        # Calculate metrics
        total_hours = monitoring_period * 24
        updates = [row[1] for row in hourly_counts]
        
        if not updates:
            return UpdateMetrics(0, 0, 0, total_hours)

        avg_updates = sum(updates) / len(updates)
        variance = sum((x - avg_updates) ** 2 for x in updates) / len(updates)
        stddev = variance ** 0.5
        
        active_hours = len([u for u in updates if u > 0])
        inactive_hours = total_hours - active_hours

        return UpdateMetrics(
            avg_hourly_updates=avg_updates,
            stddev_updates=stddev,
            active_hours=active_hours,
            inactive_hours=inactive_hours
        )

    def classify_pattern(self, table_name: str, column_name: str, 
                        metrics: Optional[UpdateMetrics] = None) -> str:
        """Classify the update pattern based on metrics."""
        # Check if we have stored metrics
        stored_metrics = self.metrics_db.get_metrics(table_name, column_name)
        if stored_metrics:
            return stored_metrics['pattern_type']
        
        # If no stored metrics, classify based on current metrics
        if metrics:
            pattern_type = ""
            if metrics.avg_hourly_updates >= 10 and metrics.active_hours >= metrics.inactive_hours:
                pattern_type = "realtime"
            elif metrics.avg_hourly_updates >= 1 and metrics.active_hours >= 20:
                pattern_type = "hourly_batch"
            elif metrics.active_hours >= 1:
                pattern_type = "daily_batch"
            else:
                pattern_type = "irregular"
            
            # Store the metrics and pattern
            self.metrics_db.store_metrics(
                table_name, column_name, pattern_type, asdict(metrics)
            )
            return pattern_type
        
        return "irregular"  # default if no metrics available

    def check_staleness(self, table_name: str, column_name: str, 
                       current_delay: float) -> bool:
        """Check if current data is stale based on historical patterns."""
        return self.metrics_db.check_staleness(table_name, column_name, current_delay)
