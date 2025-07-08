import yaml
import polars as pl
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dbqt.connections import create_connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_row_count_for_table(config, table_name):
    """Get row count for a single table using its own connector."""
    connector = create_connector(config['connection'])
    try:
        connector.connect()
        count = connector.count_rows(table_name)
        logger.info(f"Table {table_name}: {count} rows")
        return table_name, count
    except Exception as e:
        logger.error(f"Error getting count for {table_name}: {str(e)}")
        return table_name, -1
    finally:
        connector.disconnect()

def get_table_stats(config_path: str):
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Read tables CSV using polars
    df = pl.read_csv(config['tables_file'])
    table_names = df['table_name'].to_list()
    
    # Use ThreadPoolExecutor to process up to 10 tables concurrently
    row_counts = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all tasks
        future_to_table = {
            executor.submit(get_row_count_for_table, config, table_name): table_name 
            for table_name in table_names
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_table):
            table_name, count = future.result()
            row_counts[table_name] = count
    
    # Create ordered list of row counts matching the original table order
    ordered_row_counts = [row_counts[table_name] for table_name in table_names]
    
    # Add row counts to dataframe and save
    df = df.with_columns(pl.Series("row_count", ordered_row_counts))
    df.write_csv(config['tables_file'])
    
    logger.info(f"Updated row counts in {config['tables_file']}")

def main(args=None):
    import argparse
    parser = argparse.ArgumentParser(
        description='Get row counts for database tables specified in a config file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example config.yaml:
    connection:
        type: Snowflake
        user: myuser
        password: mypass
        host: myorg.snowflakecomputing.com
    tables_file: tables.csv
        """
    )
    parser.add_argument('config_file', help='YAML config file containing database connection and tables list')
    
    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)
    
    get_table_stats(args.config_file)

if __name__ == "__main__":
    main()
