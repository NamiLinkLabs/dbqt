import polars as pl
import logging
import threading
from dbqt.tools.utils import load_config, ConnectionPool, setup_logging, Timer

logger = logging.getLogger(__name__)


def get_row_count_for_table(connector, table_name):
    """Get row count for a single table using a shared connector."""
    # Set a more descriptive thread name
    threading.current_thread().name = f"Table-{table_name}"

    try:
        count = connector.count_rows(table_name)
        logger.info(f"Table {table_name}: {count} rows")
        return table_name, (count, None)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error getting count for {table_name}: {error_msg}")
        return table_name, (None, error_msg)


def get_table_stats(config_path: str):
    with Timer("Database statistics collection"):
        # Load config
        config = load_config(config_path)

        # Read tables CSV using polars
        df = pl.read_csv(config["tables_file"])
        max_workers = config.get("max_workers", 4)

        if "source_table" in df.columns and "target_table" in df.columns:
            source_tables = df["source_table"].to_list()
            target_tables = df["target_table"].to_list()
            # Create deterministic list while preserving uniqueness
            seen = set()
            table_names = []
            for table in source_tables + target_tables:
                if table not in seen:
                    table_names.append(table)
                    seen.add(table)

            with ConnectionPool(config, max_workers) as pool:
                results = pool.execute_parallel(get_row_count_for_table, table_names)

            # Separate row counts and error messages
            source_row_counts = []
            source_notes = []
            target_row_counts = []
            target_notes = []

            for table in source_tables:
                count, error = results[table]
                source_row_counts.append(count)
                source_notes.append(error)

            for table in target_tables:
                count, error = results[table]
                target_row_counts.append(count)
                target_notes.append(error)

            df = df.with_columns(
                pl.Series("source_row_count", source_row_counts),
                pl.Series("source_notes", source_notes),
                pl.Series("target_row_count", target_row_counts),
                pl.Series("target_notes", target_notes),
            )
            cols = df.columns

            # Reorder columns
            source_rc_col = cols.pop(cols.index("source_row_count"))
            source_notes_col = cols.pop(cols.index("source_notes"))
            target_rc_col = cols.pop(cols.index("target_row_count"))
            target_notes_col = cols.pop(cols.index("target_notes"))

            cols.insert(cols.index("source_table") + 1, source_rc_col)
            cols.insert(cols.index("source_table") + 2, source_notes_col)
            cols.insert(cols.index("target_table") + 1, target_rc_col)
            cols.insert(cols.index("target_table") + 2, target_notes_col)
            df = df.select(cols)

            # Add difference and percentage difference columns
            df = df.with_columns(
                (pl.col("target_row_count") - pl.col("source_row_count")).alias(
                    "difference"
                )
            )
            df = df.with_columns(
                (((pl.col("difference") / pl.col("source_row_count")) * 100))
                .fill_nan(0.0)
                .alias("percentage_difference")
            )

        elif "table_name" in df.columns:
            table_names = df["table_name"].to_list()

            with ConnectionPool(config, max_workers) as pool:
                # Execute parallel processing
                results = pool.execute_parallel(get_row_count_for_table, table_names)

            # Separate row counts and error messages
            ordered_row_counts = []
            ordered_notes = []

            for table_name in table_names:
                count, error = results[table_name]
                ordered_row_counts.append(count)
                ordered_notes.append(error)

            # Add row counts and notes to dataframe
            df = df.with_columns(
                pl.Series("row_count", ordered_row_counts),
                pl.Series("notes", ordered_notes),
            )
        else:
            logger.error(
                "CSV file must contain either 'table_name' column or 'source_table' and 'target_table' columns."
            )
            return

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
