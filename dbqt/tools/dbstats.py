import polars as pl
import logging
import threading
from dbqt.tools.utils import load_config, ConnectionPool, setup_logging

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
    config = load_config(config_path)

    # Read tables CSV using polars
    df = pl.read_csv(config["tables_file"])
    table_names = df["table_name"].to_list()

    max_workers = config.get("max_workers", 4)

    with ConnectionPool(config, max_workers) as pool:
        # Execute parallel processing
        row_counts = pool.execute_parallel(get_row_count_for_table, table_names)

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
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    setup_logging(args.verbose)
    get_table_stats(args.config)


if __name__ == "__main__":
    main()
