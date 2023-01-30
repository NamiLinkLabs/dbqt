"""Alert generation and handling."""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import logging

@dataclass
class Alert:
    """Data quality alert."""
    table: str
    column: str
    alert_type: str
    message: str
    severity: str
    timestamp: datetime
    details: Optional[Dict] = None

class AlertGenerator:
    """Generates and handles data quality alerts."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def evaluate_freshness_check(self, check_result: Dict, config: Dict) -> Optional[Alert]:
        """Evaluate freshness check results and generate alert if needed."""
        
        delay = check_result['delay']
        recent_updates = check_result.get('recent_updates', 0)
        threshold = config['monitoring_config']['alert_threshold']
        min_updates = config['monitoring_config'].get('min_expected_updates')

        alert = None
        if delay > threshold:
            alert = Alert(
                table=config['table'],
                column=config['column'],
                alert_type='freshness_delay',
                message=f"Data is {delay} behind. Threshold: {threshold}",
                severity='critical',
                timestamp=datetime.now(),
                details={
                    'delay': str(delay),
                    'threshold': threshold,
                    'recent_updates': recent_updates
                }
            )
        elif min_updates and recent_updates < min_updates:
            alert = Alert(
                table=config['table'],
                column=config['column'],
                alert_type='low_update_count',
                message=f"Only {recent_updates} updates. Expected: {min_updates}",
                severity='warning',
                timestamp=datetime.now(),
                details={
                    'recent_updates': recent_updates,
                    'min_expected': min_updates
                }
            )

        if alert:
            self.handle_alert(alert)
            
        return alert

    def handle_alert(self, alert: Alert):
        """Handle generated alert - log and dispatch notifications."""
        self.logger.warning(
            f"Data Quality Alert: {alert.alert_type} - {alert.message}",
            extra={
                'table': alert.table,
                'column': alert.column,
                'severity': alert.severity,
                'details': alert.details
            }
        )
        # TODO: Add notification dispatch (email, Slack, etc)
