import logging
from itertools import combinations
from typing import List, Tuple, Set
from datetime import datetime
from dbqt.tools.utils import load_config, setup_logging, Timer
from dbqt.connections import create_connector

logger = logging.getLogger(__name__)


def get_column_names(connector, table_name: str) -> List[str]:
    """Retrieve column names from the table"""
    try:
        metadata = connector.fetch_table_metadata(table_name)
        return [col[0] for col in metadata]
    except Exception as e:
        logger.error(f"Failed to get columns for {table_name}: {str(e)}")
        raise


def get_row_count(connector, table_name: str) -> int:
    """Get total row count"""
    try:
        return connector.count_rows(table_name)
    except Exception as e:
        logger.error(f"Failed to get row count for {table_name}: {str(e)}")
        raise


def check_key_candidate(
    connector, table_name: str, columns: Tuple[str, ...], total_rows: int
) -> bool:
    """Check if column combination is a valid key"""
    col_list = ", ".join([f'"{col}"' for col in columns])

    # First check if any of the key columns contain NULLs
    null_conditions = " OR ".join([f'"{col}" IS NULL' for col in columns])
    null_check_query = f"""
        SELECT COUNT(*) as null_count
        FROM {table_name}
        WHERE {null_conditions}
    """

    try:
        null_result = connector.run_query(null_check_query)
        null_count = null_result[0][0] if null_result else 0

        # If there are NULLs in any key column, it's not a valid key
        if null_count > 0:
            logger.debug(
                f"Columns {columns} contain {null_count} NULL values, not a valid key"
            )
            return False

        # Use a subquery with GROUP BY to count distinct combinations
        query = f"""
            SELECT COUNT(*) as distinct_count
            FROM (
                SELECT {col_list}
                FROM {table_name}
                GROUP BY {col_list}
            ) subquery
        """

        result = connector.run_query(query)
        distinct_count = result[0][0] if result else 0

        # Valid key if distinct count equals total rows
        is_valid = distinct_count == total_rows

        if not is_valid:
            logger.debug(
                f"Columns {columns}: {distinct_count:,} distinct vs {total_rows:,} total rows"
            )

        return is_valid

    except Exception as e:
        logger.error(f"Error checking key candidate {columns}: {str(e)}")
        raise


def calculate_total_combinations(n_columns: int, max_size: int = None) -> int:
    """Calculate total number of combinations"""
    if max_size is None or max_size >= n_columns:
        return 2**n_columns - 1

    from math import comb

    return sum(comb(n_columns, k) for k in range(1, max_size + 1))


def is_id_column(column_name: str) -> bool:
    """Check if column name looks like an ID column"""
    lower_name = column_name.lower()
    return (
        lower_name.startswith("id_")
        or "_id_" in lower_name
        or lower_name.endswith("_id")
        or lower_name == "id"
    )


def prioritize_id_columns(columns: List[str]) -> List[str]:
    """Sort columns to prioritize ID-like columns first"""
    id_columns = [col for col in columns if is_id_column(col)]
    non_id_columns = [col for col in columns if not is_id_column(col)]
    return id_columns + non_id_columns


def find_composite_keys(
    connector,
    table_name: str,
    columns: List[str],
    max_key_size: int = None,
    verbose: bool = False,
) -> List[Tuple[str, ...]]:
    """Find all minimal composite keys"""

    total_rows = get_row_count(connector, table_name)

    if total_rows == 0:
        logger.warning("Table is empty")
        return []

    logger.info(f"Total rows: {total_rows:,}")
    logger.info(f"Columns to analyze: {len(columns)}")

    if max_key_size is None:
        max_key_size = len(columns)

    # Prioritize ID columns
    prioritized_columns = prioritize_id_columns(columns)
    id_count = sum(1 for col in columns if is_id_column(col))
    if id_count > 0:
        logger.info(f"Found {id_count} ID-like column(s), checking those first")

    found_keys = []
    checked_combinations = 0
    excluded_supersets = set()

    # Check combinations by size (starting from 1)
    for size in range(1, min(max_key_size + 1, len(columns) + 1)):
        logger.info(f"Checking combinations of size {size}...")

        # Generate combinations using prioritized column order
        size_combinations = list(combinations(prioritized_columns, size))
        logger.info(f"Total combinations: {len(size_combinations):,}")

        for i, col_combo in enumerate(size_combinations, 1):
            # Skip if this is a superset of an already found key
            if any(set(key).issubset(set(col_combo)) for key in found_keys):
                continue

            # Skip if superset of excluded combination
            if any(
                excluded.issubset(set(col_combo)) for excluded in excluded_supersets
            ):
                continue

            checked_combinations += 1

            if verbose and i % 100 == 0:
                logger.info(f"Progress: {i}/{len(size_combinations)}")

            try:
                is_key = check_key_candidate(
                    connector, table_name, col_combo, total_rows
                )

                if is_key:
                    found_keys.append(col_combo)
                    logger.info(f"Found key: {', '.join(col_combo)}")

            except Exception as e:
                logger.error(f"Error checking {col_combo}: {e}")

        # If we found keys of this size, don't check larger sizes
        # (we only want minimal keys)
        if found_keys:
            logger.info(f"Found minimal keys of size {size}, stopping search")
            break

    logger.info(f"Total combinations checked: {checked_combinations:,}")
    return found_keys


def keyfinder(
    config_path: str,
    table_name: str,
    max_key_size: int = None,
    max_columns: int = 20,
    exclude_columns: List[str] = None,
    include_columns: List[str] = None,
    force: bool = False,
    verbose: bool = False,
):
    """Find composite keys in a database table"""

    with Timer("Composite key search"):
        # Load config
        config = load_config(config_path)

        # Create connector
        connector = create_connector(config["connection"])
        connector.connect()

        try:
            # Get columns
            columns = get_column_names(connector, table_name)

            if not columns:
                logger.error(f"No columns found for table {table_name}")
                return

            # Filter columns
            if include_columns:
                columns = [col for col in columns if col in include_columns]

            if exclude_columns:
                columns = [col for col in columns if col not in exclude_columns]

            if not columns:
                logger.error("No columns remaining after filters")
                return

            # Check if number of columns exceeds limit
            if len(columns) > max_columns:
                logger.warning(
                    f"Table has {len(columns)} columns, limiting to first {max_columns}. "
                    f"Use --max-columns to adjust or --include-only to specify columns"
                )
                columns = columns[:max_columns]

            # Calculate combinations
            total_combinations = calculate_total_combinations(
                len(columns), max_key_size
            )

            logger.info("=" * 60)
            logger.info(f"Table: {table_name}")
            logger.info(f"Columns to analyze: {len(columns)}")
            logger.info(f"Total possible combinations: {total_combinations:,}")
            logger.info("=" * 60)

            # Warn if too many combinations
            if total_combinations > 50000 and not force:
                logger.error(
                    f"WARNING: {total_combinations:,} combinations is very high! "
                    f"This may take a very long time. "
                    f"Consider using --max-size or --include-only to reduce search space. "
                    f"Use --force to proceed anyway"
                )
                return

            # Find keys
            keys = find_composite_keys(
                connector, table_name, columns, max_key_size, verbose
            )

            # Print results
            logger.info("=" * 60)

            if keys:
                logger.info(f"Found {len(keys)} minimal composite key(s):")
                for i, key in enumerate(keys, 1):
                    logger.info(f"{i}. ({', '.join(key)})")
            else:
                logger.info("No composite keys found")

        finally:
            connector.disconnect()


def main(args=None):
    import argparse

    parser = argparse.ArgumentParser(
        description="Find composite keys in a database table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --config config.yaml --table users
  %(prog)s --config config.yaml --table orders --max-size 3
  %(prog)s --config config.yaml --table products --exclude id created_at
  %(prog)s --config config.yaml --table data --include-only user_id date --force

Example config.yaml:
connection:
  type: Snowflake
  user: myuser
  password: mypass
  account: myaccount
  database: mydb
  schema: myschema
  warehouse: mywh
  role: myrole
        """,
    )

    parser.add_argument(
        "--config",
        required=True,
        help="YAML config file containing database connection details",
    )
    parser.add_argument("--table", required=True, help="Table name to analyze")
    parser.add_argument(
        "--max-size",
        type=int,
        help="Maximum key size to check (default: check all sizes)",
    )
    parser.add_argument(
        "--max-columns",
        type=int,
        default=20,
        help="Maximum number of columns to consider (default: 20)",
    )
    parser.add_argument("--exclude", nargs="+", help="Columns to exclude from search")
    parser.add_argument(
        "--include-only", nargs="+", help="Only include these columns in search"
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force execution even if combination count is high",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    setup_logging(args.verbose)

    keyfinder(
        config_path=args.config,
        table_name=args.table,
        max_key_size=args.max_size,
        max_columns=args.max_columns,
        exclude_columns=args.exclude,
        include_columns=args.include_only,
        force=args.force,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
