"""Monitoring configuration and check generation."""

from dataclasses import dataclass
from typing import Dict, List, Optional
import sqeleton
from sqeleton.queries.api import table, this, current_timestamp

@dataclass
class MonitoringConfig:
    """Configuration for freshness monitoring."""
    check_interval: str
    alert_threshold: str
    min_expected_updates: Optional[int] = None
    check_template: str = "freshness_check"

class MonitoringGenerator:
    """Generates monitoring configurations and checks."""

    PATTERN_CONFIGS = {
        "realtime": MonitoringConfig(
            check_interval="5 minutes",
            alert_threshold="15 minutes",
            min_expected_updates=100
        ),
        "hourly_batch": MonitoringConfig(
            check_interval="1 hour",
            alert_threshold="1 hour",
            min_expected_updates=1
        ),
        "daily_batch": MonitoringConfig(
            check_interval="6 hours", 
            alert_threshold="24 hours",
            min_expected_updates=1
        ),
        "irregular": MonitoringConfig(
            check_interval="1 day",
            alert_threshold="7 days"
        )
    }

    def __init__(self, db_connection):
        self.db = db_connection

    def generate_config(self, table_name: str, column_name: str, 
                       pattern_type: str) -> Dict:
        """Generate monitoring configuration for a timestamp column."""
        
        config = self.PATTERN_CONFIGS[pattern_type]
        
        return {
            "table": table_name,
            "column": column_name,
            "pattern_type": pattern_type,
            "monitoring_config": {
                "check_interval": config.check_interval,
                "alert_threshold": config.alert_threshold,
                "min_expected_updates": config.min_expected_updates
            },
            "check_template": config.check_template
        }

    def generate_check_query(self, table_name: str, column_name: str,
                           config: MonitoringConfig) -> str:
        """Generate SQL query for freshness check."""
        
        # Base delay calculation
        query = (
            table(table_name)
            .select(
                sqeleton.code("EXTRACT(EPOCH FROM (current_timestamp - {col}))/60 as delay", col=this[column_name]),
                this.count().as_('recent_updates')
            )
            .where(
                this[column_name] >= sqeleton.current_timestamp() - config.alert_threshold
            )
        )
        
        return query
