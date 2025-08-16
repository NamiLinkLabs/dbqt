"""Shared utilities for dbqt tools."""

import csv
import yaml
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dbqt.connections import create_connector

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
