import yaml
import polars as pl
import logging
import threading
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dbqt.connections import create_connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_row_count_for_table(connector, table_name):
    """Get row count for a single table using a shared connector."""
    # Set a more descriptive thread name
    threading.current_thread().name = f"Table-{table_name}"

    try:
        count = connector.count_rows(table_name)
        logger.info(f"Table {table_name}: {count} rows")
        return table_name, count
    except Exception as e:
        logger.error(f"Error getting count for {table_name}: {str(e)}")
        return table_name, -1


def get_table_stats(config_path: str):
    # Load config
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Read tables CSV using polars
    df = pl.read_csv(config["tables_file"])
    table_names = df["table_name"].to_list()

    # Create a pool of 10 connectors that will be reused
    max_workers = 10
    connectors = []

    logger.info(f"Creating {max_workers} database connections...")
    for i in range(max_workers):
        connector = create_connector(config["connection"])
        connector.connect()
        connectors.append(connector)

    try:
        # Use ThreadPoolExecutor with shared connectors
        row_counts = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks, cycling through available connectors
            future_to_table = {}
            for i, table_name in enumerate(table_names):
                connector = connectors[i % max_workers]  # Round-robin assignment
                future = executor.submit(get_row_count_for_table, connector, table_name)
                future_to_table[future] = table_name

            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name, count = future.result()
                row_counts[table_name] = count

    finally:
        # Clean up all connections
        logger.info("Closing database connections...")
        for connector in connectors:
            try:
                connector.disconnect()
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")

    # Create ordered list of row counts matching the original table order
    ordered_row_counts = [row_counts[table_name] for table_name in table_names]

    # Add row counts to dataframe and save
    df = df.with_columns(pl.Series("row_count", ordered_row_counts))
    df.write_csv(config["tables_file"])

    logger.info(f"Updated row counts in {config['tables_file']}")


def main(args=None):
    import argparse

    parser = argparse.ArgumentParser(
        description="Get row counts for database tables specified in a config file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example config.yaml:
    connection:
        type: Snowflake
        user: myuser
        password: mypass
        host: myorg.snowflakecomputing.com
    tables_file: tables.csv
        """,
    )
    parser.add_argument(
        "--config",
        required=True,
        help="YAML config file containing database connection and tables list",
    )

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    get_table_stats(args.config)


if __name__ == "__main__":
    main()
