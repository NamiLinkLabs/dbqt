"""Shared utilities for dbqt tools."""

import csv
import yaml
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dbqt.connections import (  # noqa: F401 — re-export
    create_connector,
    normalize_table_path,
    build_qualified_table_name,
)

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def read_csv_list(csv_path: str, column_name: str = "table_name") -> list:
    """Read a list of values from a CSV file."""
    values = []
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if row and row[0].strip():
                # Skip header if first row matches the expected column name
                if i == 0 and row[0].strip().lower() == column_name.lower():
                    continue
                values.append(row[0].strip())
    return values


class ConnectionPool:
    """Manages a pool of database connections for concurrent operations."""

    def __init__(self, config: dict, max_workers: int = 10):
        self.config = config
        self.max_workers = max_workers
        self.connectors = []
        self._lock = threading.Lock()

    def __enter__(self):
        logger.info(f"Creating {self.max_workers} database connections...")
        created_connections = 0
        try:
            for i in range(self.max_workers):
                connector = create_connector(self.config["connection"])
                connector.connect()
                self.connectors.append(connector)
                created_connections += 1
                logger.debug(
                    f"Created connection {created_connections}/{self.max_workers}"
                )
        except Exception as e:
            logger.error(
                f"Failed to create connection {created_connections + 1}: {str(e)}"
            )
            # Clean up any connections that were successfully created
            self._cleanup_connections()
            raise

        logger.info(f"Successfully created {len(self.connectors)} database connections")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup_connections()

    def _cleanup_connections(self):
        """Clean up all connections with proper error handling."""
        with self._lock:
            if not self.connectors:
                return

            logger.info(f"Closing {len(self.connectors)} database connections...")
            failed_disconnects = 0

            for i, connector in enumerate(self.connectors):
                try:
                    connector.disconnect()
                    logger.debug(f"Closed connection {i + 1}/{len(self.connectors)}")
                except Exception as e:
                    failed_disconnects += 1
                    logger.warning(f"Error closing connection {i + 1}: {str(e)}")

            if failed_disconnects > 0:
                logger.warning(f"Failed to close {failed_disconnects} connections")
            else:
                logger.info("All database connections closed successfully")

            self.connectors.clear()

    def execute_parallel(self, func, items: list) -> dict:
        """Execute a function in parallel across items using the connection pool."""
        if not self.connectors:
            raise RuntimeError("No database connections available")

        results = {}
        logger.info(
            f"Processing {len(items)} items with {len(self.connectors)} connections"
        )

        with ThreadPoolExecutor(max_workers=len(self.connectors)) as executor:
            # Submit all tasks, cycling through available connectors
            future_to_item = {}
            for i, item in enumerate(items):
                connector = self.connectors[
                    i % len(self.connectors)
                ]  # Round-robin assignment
                future = executor.submit(func, connector, item)
                future_to_item[future] = item

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                completed += 1
                try:
                    result = future.result()
                    if isinstance(result, tuple) and len(result) == 2:
                        # Handle (key, value) tuple results
                        key, value = result
                        results[key] = value
                    else:
                        results[item] = result
                    logger.debug(f"Completed {completed}/{len(items)}: {item}")
                except Exception as e:
                    logger.error(f"Error processing {item}: {str(e)}")
                    results[item] = (
                        (None, str(e))
                        if isinstance(func.__name__, str) and "count" in func.__name__
                        else None
                    )

        logger.info(f"Parallel processing completed: {len(results)} results")
        return results


def setup_logging(verbose: bool = False, format_string: str = None):
    """Setup logging configuration."""
    if format_string is None:
        format_string = (
            "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s"
        )

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=format_string,
    )


def format_runtime(seconds: float) -> str:
    """Format runtime in a human-readable format."""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.2f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        remaining_seconds = seconds % 60
        return f"{hours}h {remaining_minutes}m {remaining_seconds:.2f}s"


def discover_tables_from_db(config):
    """Connect to a database and list all tables in the configured schema.

    Returns a list of table name strings.
    """
    connector = create_connector(config["connection"])
    connector.connect()
    try:
        tables = connector.list_tables()
    finally:
        connector.disconnect()
    return tables


def _read_table_lists(tables_file, source_config=None, target_config=None):
    """Read the tables CSV and return (df, source_tables, target_tables).

    If *tables_file* is ``None``, tables are auto-discovered from the database
    schema defined in the YAML config(s).

    Returns target_tables=None when operating in single-config mode.
    """
    import polars as pl

    if tables_file is not None:
        df = pl.read_csv(tables_file)
        if "source_table" in df.columns and "target_table" in df.columns:
            return df, df["source_table"].to_list(), df["target_table"].to_list()
        elif "table_name" in df.columns:
            return df, df["table_name"].to_list(), None
        else:
            raise ValueError(
                "CSV must contain 'table_name' or 'source_table'/'target_table' columns."
            )

    # Auto-discover tables from database(s)
    logger.info("No tables_file provided — discovering tables from database schema")

    if source_config and target_config and source_config is not target_config:
        source_tables_set = set(discover_tables_from_db(source_config))
        target_tables_set = set(discover_tables_from_db(target_config))
        common = sorted(source_tables_set & target_tables_set)
        source_only = sorted(source_tables_set - target_tables_set)
        target_only = sorted(target_tables_set - source_tables_set)

        rows = []
        for t in common:
            rows.append({"source_table": t, "target_table": t, "_discovery_status": "common"})
        for t in source_only:
            rows.append({"source_table": t, "target_table": t, "_discovery_status": "source_only"})
        for t in target_only:
            rows.append({"source_table": t, "target_table": t, "_discovery_status": "target_only"})

        df = pl.DataFrame(rows)
        return (
            df,
            df["source_table"].to_list(),
            df["target_table"].to_list(),
        )
    elif source_config:
        tables = discover_tables_from_db(source_config)
        df = pl.DataFrame({"table_name": tables})
        return df, tables, None
    else:
        raise ValueError(
            "Cannot discover tables: no config provided and no tables_file specified"
        )


def get_metadata_for_table(connector, table_name, prefix=""):
    """Fetch column metadata for a single table via fetch_table_metadata."""
    import threading

    threading.current_thread().name = f"Meta-{prefix}{table_name}"
    try:
        metadata = connector.fetch_table_metadata(table_name)
        logger.info(f"Table {prefix}{table_name}: {len(metadata)} columns")
        return table_name, (metadata, None)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error getting metadata for {prefix}{table_name}: {error_msg}")
        return table_name, (None, error_msg)


def metadata_to_df(results, table_names):
    """Convert metadata results dict into a Polars DataFrame."""
    import polars as pl

    rows = []
    for table_name in table_names:
        metadata, error = results[table_name]
        if metadata is None:
            continue
        for col_row in metadata:
            rows.append(
                {
                    "SCH_TABLE": table_name,
                    "COL_NAME": str(col_row[0]).upper(),
                    "DATA_TYPE": str(col_row[1]).upper() if col_row[1] else "N/A",
                    "DATETIME_PRECISION": col_row[2] if len(col_row) > 2 else None,
                    "NUMERIC_PRECISION": col_row[3] if len(col_row) > 3 else None,
                    "NUMERIC_SCALE": col_row[4] if len(col_row) > 4 else None,
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "SCH_TABLE": pl.Utf8,
                "COL_NAME": pl.Utf8,
                "DATA_TYPE": pl.Utf8,
                "DATETIME_PRECISION": pl.Int64,
                "NUMERIC_PRECISION": pl.Int64,
                "NUMERIC_SCALE": pl.Int64,
            }
        )
    return pl.DataFrame(rows)


def fetch_metadata_parallel(config, table_names, prefix="", max_workers=4):
    """Fetch metadata for many tables in parallel, return a Polars DataFrame."""
    workers = min(max_workers, len(table_names))
    with ConnectionPool(config, workers) as pool:
        results = pool.execute_parallel(
            lambda c, t: get_metadata_for_table(c, t, prefix),
            table_names,
        )
    return metadata_to_df(results, table_names)


def fetch_all_metadata_as_df(config, table_names=None):
    """Fetch column metadata for an entire schema in ONE query, return a Polars DataFrame.

    If *table_names* is provided the result is filtered to only those tables.
    This is much faster than ``fetch_metadata_parallel`` when many tables are
    involved because it issues a single ``SELECT … FROM information_schema.columns``
    (or equivalent) instead of one query per table.
    """
    import polars as pl

    connector = create_connector(config["connection"])
    connector.connect()
    try:
        raw = connector.fetch_schema_metadata()
    finally:
        connector.disconnect()

    # raw rows: (table_name, column_name, data_type, dt_prec, num_prec, num_scale)
    if not raw:
        return pl.DataFrame(
            schema={
                "SCH_TABLE": pl.Utf8,
                "COL_NAME": pl.Utf8,
                "DATA_TYPE": pl.Utf8,
                "DATETIME_PRECISION": pl.Int64,
                "NUMERIC_PRECISION": pl.Int64,
                "NUMERIC_SCALE": pl.Int64,
            }
        )

    rows = [
        {
            "SCH_TABLE": str(r[0]).upper(),
            "COL_NAME": str(r[1]).upper(),
            "DATA_TYPE": str(r[2]).upper() if r[2] else "N/A",
            "DATETIME_PRECISION": r[3],
            "NUMERIC_PRECISION": r[4],
            "NUMERIC_SCALE": r[5],
        }
        for r in raw
    ]
    df = pl.DataFrame(rows)

    if table_names is not None:
        upper_names = {t.upper() for t in table_names}
        df = df.filter(pl.col("SCH_TABLE").is_in(upper_names))

    return df


class Timer:
    """Context manager for timing operations."""

    def __init__(self, operation_name: str = "Operation"):
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = time.time()
        logger.info(f"{self.operation_name} started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        runtime = self.end_time - self.start_time
        logger.info(f"{self.operation_name} completed in {format_runtime(runtime)}")
