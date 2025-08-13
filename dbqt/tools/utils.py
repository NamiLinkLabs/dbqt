"""Shared utilities for dbqt tools."""

import csv
import yaml
import logging
import threading
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

    def __enter__(self):
        logger.info(f"Creating {self.max_workers} database connections...")
        for i in range(self.max_workers):
            connector = create_connector(self.config["connection"])
            connector.connect()
            self.connectors.append(connector)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info("Closing database connections...")
        for connector in self.connectors:
            try:
                connector.disconnect()
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")

    def execute_parallel(self, func, items: list) -> dict:
        """Execute a function in parallel across items using the connection pool."""
        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks, cycling through available connectors
            future_to_item = {}
            for i, item in enumerate(items):
                connector = self.connectors[
                    i % self.max_workers
                ]  # Round-robin assignment
                future = executor.submit(func, connector, item)
                future_to_item[future] = item

            # Collect results as they complete
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                    if isinstance(result, tuple) and len(result) == 2:
                        # Handle (key, value) tuple results
                        key, value = result
                        results[key] = value
                    else:
                        results[item] = result
                except Exception as e:
                    logger.error(f"Error processing {item}: {str(e)}")
                    results[item] = None

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
