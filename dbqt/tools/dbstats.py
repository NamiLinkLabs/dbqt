import polars as pl
import logging
import threading
from dbqt.tools.utils import load_config, ConnectionPool, setup_logging, Timer

logger = logging.getLogger(__name__)


def get_row_count_for_table(connector, table_name, prefix=""):
    """Get row count for a single table using a shared connector."""
    # Set a more descriptive thread name
    threading.current_thread().name = f"Table-{prefix}{table_name}"

    try:
        count = connector.count_rows(table_name)
        logger.info(f"Table {prefix}{table_name}: {count} rows")
        return table_name, (count, None)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error getting count for {prefix}{table_name}: {error_msg}")
        return table_name, (None, error_msg)


def get_table_stats(
    config_path: str, source_config_path: str = None, target_config_path: str = None
):
    with Timer("Database statistics collection"):
        # Load config(s)
        if source_config_path and target_config_path:
            # Two separate configs for source and target
            source_config = load_config(source_config_path)
            target_config = load_config(target_config_path)
            # Use tables_file from source config (or target if not in source)
            tables_file = source_config.get("tables_file") or target_config.get(
                "tables_file"
            )
            max_workers = source_config.get("max_workers", 4)
        else:
            # Single config for both source and target
            config = load_config(config_path)
            source_config = target_config = config
            tables_file = config["tables_file"]
            max_workers = config.get("max_workers", 4)

        # Read tables CSV using polars
        df = pl.read_csv(tables_file)

        if "source_table" in df.columns and "target_table" in df.columns:
            source_tables = df["source_table"].to_list()
            target_tables = df["target_table"].to_list()

            # Limit workers to number of tables to avoid creating unnecessary connections
            source_workers = min(max_workers, len(source_tables))
            target_workers = min(max_workers, len(target_tables))

            # Process source and target tables separately with their respective configs
            with ConnectionPool(source_config, source_workers) as source_pool:
                source_results = source_pool.execute_parallel(
                    lambda connector, table: get_row_count_for_table(
                        connector, table, "source:"
                    ),
                    source_tables,
                )

            with ConnectionPool(target_config, target_workers) as target_pool:
                target_results = target_pool.execute_parallel(
                    lambda connector, table: get_row_count_for_table(
                        connector, table, "target:"
                    ),
                    target_tables,
                )

            # Separate row counts and error messages
            source_row_counts = []
            source_notes = []
            target_row_counts = []
            target_notes = []

            for table in source_tables:
                count, error = source_results[table]
                source_row_counts.append(count)
                source_notes.append(error)

            for table in target_tables:
                count, error = target_results[table]
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

            # Limit workers to number of tables to avoid creating unnecessary connections
            actual_workers = min(max_workers, len(table_names))

            with ConnectionPool(source_config, actual_workers) as pool:
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

        df.write_csv(tables_file)

        logger.info(f"Updated row counts in {tables_file}")


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
        help="YAML config file containing database connection and tables list (used for both source and target if --source-config and --target-config not provided)",
    )
    parser.add_argument(
        "--source-config",
        help="YAML config file for source database connection",
    )
    parser.add_argument(
        "--target-config",
        help="YAML config file for target database connection",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    setup_logging(args.verbose)

    # Validate arguments
    if args.source_config and args.target_config:
        # Two-config mode
        get_table_stats(None, args.source_config, args.target_config)
    elif args.config:
        # Single-config mode
        get_table_stats(args.config)
    else:
        parser.error(
            "Either --config or both --source-config and --target-config must be provided"
        )


if __name__ == "__main__":
    main()
