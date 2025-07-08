import yaml
import polars as pl
import logging
from dbqt.connections import create_connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_table_stats(config_path: str):
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Read tables CSV using polars
    df = pl.read_csv(config['tables_file'])
    
    # Connect to database
    connector = create_connector(config['connection'])
    connector.connect()
    
    # Get row counts
    row_counts = []
    for table_name in df['table_name']:
        try:
            count = connector.count_rows(table_name)
            row_counts.append(count)
            logger.info(f"Table {table_name}: {count} rows")
        except Exception as e:
            logger.error(f"Error getting count for {table_name}: {str(e)}")
            row_counts.append(-1)
    
    connector.disconnect()
    
    # Add row counts to dataframe and save
    df = df.with_columns(pl.Series("row_count", row_counts))
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
