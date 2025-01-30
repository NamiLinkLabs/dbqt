"""Example script demonstrating data quality monitoring on test_db."""

import logging
import sqeleton
from src.schema import SchemaDiscovery
from src.patterns import PatternAnalyzer
from src.monitoring import MonitoringGenerator
from src.alerts import AlertGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Database connection using sqeleton
    db = sqeleton.connect('postgresql://postgres:postgres@localhost:5432/test_db')
    
    try:
        # Initialize components
        schema_discovery = SchemaDiscovery(db)
        pattern_analyzer = PatternAnalyzer(db)
        monitoring_gen = MonitoringGenerator(db)
        alert_gen = AlertGenerator()

        # Example: Analyze realtime_updates table
        table_name = "realtime_updates"
        table_meta = schema_discovery.get_table_metadata(table_name)
        
        # Find timestamp columns
        timestamp_cols = schema_discovery.discover_timestamp_columns(table_meta)
        
        for col_name in timestamp_cols:
            logger.info(f"Analyzing {table_name}.{col_name}")
            
            # Calculate metrics and classify pattern
            metrics = pattern_analyzer.calculate_metrics(table_name, col_name)
            pattern = pattern_analyzer.classify_pattern(table_name, col_name, metrics)
            
            logger.info(f"Detected pattern: {pattern}")
            logger.info(f"Metrics: {metrics}")
            
            # Generate monitoring config
            config = monitoring_gen.generate_config(table_name, col_name, pattern)
            
            # Run a sample check
            check_query = monitoring_gen.generate_check_query(
                table_name, col_name, config['monitoring_config']
            )
            result = db.query(check_query)
            
            # Evaluate results
            if result:
                alert = alert_gen.evaluate_freshness_check(
                    {'delay': result[0], 'recent_updates': result[1]},
                    config
                )
                if alert:
                    logger.warning(f"Alert generated: {alert}")
                else:
                    logger.info("No alerts generated - data is fresh")

    except Exception as e:
        logger.error(f"Error monitoring test_db: {str(e)}")
        raise

if __name__ == "__main__":
    main()
