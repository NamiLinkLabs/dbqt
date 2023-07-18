import yaml
import pandas as pd
import logging
from src.connectors import get_connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_table_stats(config_path: str):
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Read tables CSV
    tables_df = pd.read_csv(config['tables_file'])
    
    # Connect to database
    connector = get_connector(config['connection'])
    connector.connect()
    
    # Get row counts
    row_counts = []
    for table in tables_df['table_name']:
        try:
            count = connector.get_row_count(table)
            row_counts.append(count)
            logger.info(f"Table {table}: {count} rows")
        except Exception as e:
            logger.error(f"Error getting count for {table}: {str(e)}")
            row_counts.append(-1)
    
    connector.disconnect()
    
    # Update CSV with row counts
    tables_df['row_count'] = row_counts
    tables_df.to_csv(config['tables_file'], index=False)
    
    logger.info(f"Updated row counts in {config['tables_file']}")

def main():
    import sys
    if len(sys.argv) != 2:
        print("Usage: python dbstats.py <config_file>")
        sys.exit(1)
        
    get_table_stats(sys.argv[1])

if __name__ == "__main__":
    main()
