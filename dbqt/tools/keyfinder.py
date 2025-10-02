import argparse
import logging
import polars as pl
from itertools import combinations
from typing import List, Tuple, Optional
from dbqt.tools.utils import load_config, setup_logging, Timer
from dbqt.connections import create_connector
from math import comb

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
    col_list = ", ".join(columns)

    # First check if any of the key columns contain NULLs
    null_conditions = " OR ".join([f"{col} IS NULL" for col in columns])
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
    is_temp_table: bool = False,
) -> List[Tuple[str, ...]]:
    """Find all minimal composite keys"""

    total_rows = get_row_count(connector, table_name)

    if not total_rows or total_rows == 0:
        logger.warning(f"Table {table_name} is empty")
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


def find_keys_for_table(
    connector,
    table_name: str,
    max_key_size: int = None,
    max_columns: int = 20,
    exclude_columns: List[str] = None,
    include_columns: List[str] = None,
    force: bool = False,
    verbose: bool = False,
    sample_size: int = None,
) -> Tuple[str, Tuple[Optional[str], Optional[str], Optional[str], bool]]:
    """Find composite keys for a single table and return formatted result

    Returns:
        Tuple of (table_name, (primary_key, error, dedup_table, dedup_needed))
        where dedup_needed is True if deduplication was required
    """
    # Track if we created a sample table that needs cleanup
    sample_table_name = None
    original_table_name = table_name

    try:
        # Create sample table if sample_size is specified
        if sample_size:
            sample_table_name = f"temp_sample_{table_name.replace('.', '_')}"
            logger.info(
                f"Creating sample table with {sample_size:,} records: {sample_table_name}"
            )

            try:
                sample_query = f"""
                    CREATE OR REPLACE TABLE {sample_table_name} AS
                    SELECT *
                    FROM {table_name}
                    LIMIT {sample_size}
                """
                connector.run_query(sample_query)

                # Check if sample table has any rows
                sample_row_count = connector.count_rows(sample_table_name)
                if sample_row_count == 0:
                    logger.warning(f"Sample table is empty for {table_name}")
                    # Clean up empty sample table
                    connector.run_query(f"DROP TABLE IF EXISTS {sample_table_name}")
                    return table_name, (None, "Source table is empty", None, False)

                logger.info(f"Sample table created with {sample_row_count:,} rows")
                # Use the sample table for analysis
                table_name = sample_table_name

            except Exception as sample_error:
                logger.error(f"Failed to create sample table: {sample_error}")
                return original_table_name, (
                    None,
                    f"Failed to create sample table: {str(sample_error)}",
                    None,
                    False,
                )

        # Get columns
        columns = get_column_names(connector, table_name)

        if not columns:
            logger.error(f"No columns found for table {table_name}")
            return table_name, (None, f"No columns found", None, False)

        # Filter columns
        if include_columns:
            columns = [col for col in columns if col in include_columns]

        if exclude_columns:
            columns = [col for col in columns if col not in exclude_columns]

        if not columns:
            logger.error(f"No columns remaining after filters for {table_name}")
            return table_name, (None, "No columns after filters", None, False)

        # Check if number of columns exceeds limit
        if len(columns) > max_columns:
            logger.warning(
                f"Table {table_name} has {len(columns)} columns, limiting to first {max_columns}"
            )
            columns = columns[:max_columns]

        # Calculate combinations
        total_combinations = calculate_total_combinations(len(columns), max_key_size)

        logger.info(
            f"Table {table_name}: {len(columns)} columns, {total_combinations:,} combinations"
        )

        # Warn if too many combinations
        if total_combinations > 50000 and not force:
            error_msg = f"Too many combinations ({total_combinations:,}), use --force"
            logger.error(f"{table_name}: {error_msg}")
            return table_name, (None, error_msg, None, False)

        # Find keys
        keys = find_composite_keys(
            connector, table_name, columns, max_key_size, verbose
        )

        # Check if table was empty (find_composite_keys returns empty list)
        total_rows = get_row_count(connector, table_name)
        if total_rows == 0:
            logger.warning(f"Table {table_name} is empty")
            return table_name, (None, "Table is empty", None, False)

        if keys:
            # Use the first (minimal) key found
            primary_key = keys[0]
            # Format as comma-separated list
            formatted_key = ", ".join(primary_key)
            logger.info(f"Found key for {table_name}: {formatted_key}")
            return table_name, (formatted_key, None, None, False)
        else:
            logger.warning(f"No composite keys found for {table_name}")

            # Try with deduplicated table
            logger.info(
                f"Attempting to find key on deduplicated version of {table_name}"
            )
            # Sanitize table name for dedup table (handle None and special characters)
            dedup_table_name = f"dedup_{table_name.replace('.', '_')}"
            try:
                # Create deduplicated table and find keys in a single connection context
                col_list = ", ".join(columns)
                dedup_query = f"""
                    CREATE OR REPLACE TABLE {dedup_table_name} AS
                    SELECT {col_list}
                    FROM {table_name}
                    GROUP BY {col_list}
                """

                logger.info(f"Creating deduplicated table: {dedup_table_name}")
                connector.run_query(dedup_query)

                # Get row count for the dedup table
                try:
                    dedup_row_count = connector.count_rows(dedup_table_name)
                    logger.info(f"Deduplicated table has {dedup_row_count:,} rows")

                    if dedup_row_count == 0:
                        logger.warning(f"Deduplicated table is empty for {table_name}")
                        # Drop the empty dedup table
                        try:
                            connector.run_query(
                                f"DROP TABLE IF EXISTS {dedup_table_name}"
                            )
                        except Exception as cleanup_error:
                            logger.warning(
                                f"Failed to drop empty dedup table {dedup_table_name}: {cleanup_error}"
                            )
                        return table_name, (
                            None,
                            "No keys found (deduplicated table is empty)",
                            None,
                            False,
                        )

                    # Now check combinations directly using the same connector
                    dedup_keys = []
                    checked_combinations = 0

                    # Prioritize ID columns
                    prioritized_columns = prioritize_id_columns(columns)

                    # Check combinations by size
                    for size in range(
                        1, min((max_key_size or len(columns)) + 1, len(columns) + 1)
                    ):
                        logger.info(
                            f"Checking deduplicated combinations of size {size}..."
                        )

                        size_combinations = list(
                            combinations(prioritized_columns, size)
                        )

                        for col_combo in size_combinations:
                            checked_combinations += 1

                            try:
                                is_key = check_key_candidate(
                                    connector,
                                    dedup_table_name,
                                    col_combo,
                                    dedup_row_count,
                                )

                                if is_key:
                                    dedup_keys.append(col_combo)
                                    logger.info(
                                        f"Found key in deduplicated table: {', '.join(col_combo)}"
                                    )
                                    break

                            except Exception as e:
                                logger.error(
                                    f"Error checking {col_combo} on dedup table: {e}"
                                )

                        if dedup_keys:
                            break

                except Exception as count_error:
                    logger.error(
                        f"Error getting row count for dedup table: {count_error}"
                    )
                    # Drop the dedup table on error
                    try:
                        connector.run_query(f"DROP TABLE IF EXISTS {dedup_table_name}")
                    except Exception as cleanup_error:
                        logger.warning(
                            f"Failed to drop dedup table {dedup_table_name}: {cleanup_error}"
                        )
                    return table_name, (
                        None,
                        f"No keys found (dedup table error: {str(count_error)})",
                        None,
                        False,
                    )

                if dedup_keys:
                    # Found a key in deduplicated version - keep the materialized table
                    primary_key = dedup_keys[0]
                    formatted_key = ", ".join(primary_key)
                    note = f"Primary key found: {formatted_key}, but table has duplicate records - using deduplicated table"
                    logger.warning(f"{table_name}: {note}")
                    # Return the dedup table name and flag that dedup was needed
                    return table_name, (formatted_key, note, dedup_table_name, True)
                else:
                    # No key found even after dedup - clean up the dedup table
                    try:
                        connector.run_query(f"DROP TABLE IF EXISTS {dedup_table_name}")
                    except Exception as cleanup_error:
                        logger.warning(
                            f"Failed to drop dedup table {dedup_table_name}: {cleanup_error}"
                        )

                    logger.warning(
                        f"No keys found even after deduplication for {table_name}"
                    )
                    return table_name, (
                        None,
                        "No keys found (even after deduplication)",
                        None,
                        False,
                    )

            except Exception as dedup_error:
                logger.error(
                    f"Error during deduplication attempt for {table_name}: {dedup_error}"
                )
                return table_name, (
                    None,
                    f"No keys found (deduplication failed: {str(dedup_error)})",
                    None,
                    False,
                )

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error processing {original_table_name}: {error_msg}")
        return original_table_name, (None, error_msg, None, False)

    finally:
        # Clean up sample table if it was created
        if sample_table_name:
            try:
                connector.run_query(f"DROP TABLE IF EXISTS {sample_table_name}")
                logger.debug(f"Dropped sample table: {sample_table_name}")
            except Exception as cleanup_error:
                logger.warning(
                    f"Failed to drop sample table {sample_table_name}: {cleanup_error}"
                )


def cleanup_temp_tables(connector):
    """Drop all temp_dedup_* tables/views from the database

    Args:
        connector: Database connector
    """
    try:
        # Query to find all temp_dedup_* tables and views
        query = """
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_name LIKE 'temp_dedup_%'
        """

        try:
            result = connector.run_query(query)

            if result:
                logger.info(f"Dropping {len(result)} temp_dedup_* objects")
                for row in result:
                    table_name = row[0]
                    table_type = row[1]
                    drop_type = "VIEW" if table_type == "VIEW" else "TABLE"
                    try:
                        connector.run_query(f"DROP {drop_type} IF EXISTS {table_name}")
                        logger.debug(f"Dropped {drop_type.lower()}: {table_name}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to drop {drop_type.lower()} {table_name}: {e}"
                        )

        except Exception as e:
            # If information_schema query fails, try alternative approach for DuckDB
            logger.debug(f"Standard cleanup query failed, trying alternative: {e}")
            try:
                # DuckDB-specific query
                result = connector.run_query("SHOW TABLES")
                temp_objects = (
                    [row[0] for row in result if row[0].startswith("temp_dedup_")]
                    if result
                    else []
                )

                if temp_objects:
                    logger.info(f"Dropping {len(temp_objects)} temp_dedup_* objects")
                    for obj_name in temp_objects:
                        # Try both TABLE and VIEW
                        for drop_type in ["TABLE", "VIEW"]:
                            try:
                                connector.run_query(
                                    f"DROP {drop_type} IF EXISTS {obj_name}"
                                )
                                logger.debug(f"Dropped {drop_type.lower()}: {obj_name}")
                                break
                            except Exception:
                                continue

            except Exception as e2:
                logger.warning(f"Could not clean up temp objects: {e2}")

    except Exception as e:
        logger.warning(f"Error during temp object cleanup: {e}")


def keyfinder(
    config_path: str,
    table_name: str = None,
    tables_file: str = None,
    max_key_size: int = None,
    max_columns: int = 20,
    exclude_columns: List[str] = None,
    include_columns: List[str] = None,
    force: bool = False,
    verbose: bool = False,
    sample_size: int = None,
):
    """Find composite keys in database table(s)"""

    with Timer("Composite key search"):
        # Load config
        config = load_config(config_path)

        # Single table mode
        if table_name:
            connector = create_connector(config["connection"])
            connector.connect()

            try:
                _, (primary_key, error, dedup_table, _) = find_keys_for_table(
                    connector,
                    table_name,
                    max_key_size,
                    max_columns,
                    exclude_columns,
                    include_columns,
                    force,
                    verbose,
                    sample_size,
                )

                logger.info("=" * 60)
                if primary_key:
                    logger.info(f"Primary key for {table_name}: {primary_key}")
                    if dedup_table:
                        logger.info(f"Using deduplicated table: {dedup_table}")
                        logger.info(
                            f"Note: Deduplicated table has been materialized for use in comparisons"
                        )
                else:
                    logger.info(f"No key found for {table_name}: {error}")
                logger.info("=" * 60)

            finally:
                # Clean up any temp objects (dedup tables are kept, temp_dedup_* are removed)
                cleanup_temp_tables(connector)
                connector.disconnect()

        # Multiple tables mode
        else:
            # Get tables_file from argument or config
            if not tables_file:
                tables_file = config.get("tables_file")

            if not tables_file:
                logger.error(
                    "tables_file must be provided via --tables-file argument or in config YAML"
                )
                return

            df = pl.read_csv(tables_file)

            # Determine which column contains table names
            has_source_target = (
                "source_table" in df.columns and "target_table" in df.columns
            )

            if "table_name" in df.columns:
                table_column = "table_name"
                tables = df["table_name"].to_list()
                source_tables = None
                target_tables = None
            elif has_source_target:
                # Process both source and target tables
                source_tables = df["source_table"].to_list()
                target_tables = df["target_table"].to_list()

                # For initial processing, collect unique tables from both columns
                tables = []
                for source, target in zip(source_tables, target_tables):
                    if source and str(source) != "null":
                        tables.append(source)
                    elif target and str(target) != "null":
                        tables.append(target)
                    else:
                        tables.append(None)
                table_column = "source_target"  # Flag for dual column mode
            else:
                logger.error(
                    "CSV file must contain either 'table_name' or 'source_table'/'target_table' columns"
                )
                return

            # Get unique tables to process (for source/target mode, we'll process both)
            if has_source_target:
                # Collect all unique non-null tables from both columns
                unique_tables = set()
                for source, target in zip(source_tables, target_tables):
                    if source and str(source) != "null":
                        unique_tables.add(source)
                    if target and str(target) != "null":
                        unique_tables.add(target)
                valid_tables = list(unique_tables)
            else:
                # Filter out None values
                valid_tables = [t for t in tables if t is not None]

            if not valid_tables:
                logger.error("No valid tables found in CSV file")
                return

            logger.info(f"Processing {len(valid_tables)} tables sequentially...")

            # Process tables sequentially with a single connection
            connector = create_connector(config["connection"])
            connector.connect()

            # Clean up any existing temp objects at start
            cleanup_temp_tables(connector)

            results = {}

            try:
                for i, table in enumerate(valid_tables, 1):
                    logger.info(f"Processing table {i}/{len(valid_tables)}: {table}")
                    table_name, result = find_keys_for_table(
                        connector,
                        table,
                        max_key_size,
                        max_columns,
                        exclude_columns,
                        include_columns,
                        force,
                        verbose,
                        sample_size,
                    )
                    results[table_name] = result

                # If we have source/target mode and deduplication was needed for source,
                # create dedup table for target as well
                if has_source_target:
                    for source, target in zip(source_tables, target_tables):
                        # Skip if either is None/null
                        if (
                            not source
                            or str(source) == "null"
                            or not target
                            or str(target) == "null"
                        ):
                            continue

                        # Check if source needed deduplication
                        if source in results:
                            _, _, source_dedup_table, source_dedup_needed = results[
                                source
                            ]

                            # If source needed dedup and target hasn't been processed yet or didn't need dedup
                            if source_dedup_needed and target in results:
                                (
                                    target_key,
                                    target_error,
                                    target_dedup_table,
                                    target_dedup_needed,
                                ) = results[target]

                                # If target found a key but didn't need dedup, create dedup table for consistency
                                if target_key and not target_dedup_needed:
                                    logger.info(
                                        f"Source table {source} needed deduplication, creating dedup table for target {target} as well"
                                    )

                                    # Get columns for target table
                                    try:
                                        target_columns = get_column_names(
                                            connector, target
                                        )

                                        if target_columns:
                                            # Create dedup table for target
                                            target_dedup_name = (
                                                f"dedup_{target.replace('.', '_')}"
                                            )
                                            col_list = ", ".join(target_columns)
                                            dedup_query = f"""
                                                CREATE OR REPLACE TABLE {target_dedup_name} AS
                                                SELECT {col_list}
                                                FROM {target}
                                                GROUP BY {col_list}
                                            """

                                            connector.run_query(dedup_query)
                                            logger.info(
                                                f"Created deduplicated table: {target_dedup_name}"
                                            )

                                            # Update results with dedup table info
                                            note = f"Primary key found: {target_key}, deduplicated for consistency with source table"
                                            results[target] = (
                                                target_key,
                                                note,
                                                target_dedup_name,
                                                True,
                                            )

                                    except Exception as e:
                                        logger.error(
                                            f"Failed to create dedup table for target {target}: {e}"
                                        )

            finally:
                # Clean up any temp objects before disconnecting (dedup_ tables are kept)
                cleanup_temp_tables(connector)
                connector.disconnect()

            # Build results lists maintaining original order
            if table_column == "table_name":
                # Single table column mode
                primary_keys = []
                notes = []
                dedup_tables = []

                for table in tables:
                    if table is None or table not in results:
                        primary_keys.append(None)
                        notes.append("No table specified")
                        dedup_tables.append(None)
                    else:
                        key, error, dedup_table, _ = results[table]
                        primary_keys.append(key)
                        notes.append(error)
                        dedup_tables.append(dedup_table)

                # Find position of table_name column
                table_name_idx = df.columns.index("table_name")
                # Create new column order: everything up to and including table_name, then dedup_table, then rest
                new_columns = (
                    df.columns[: table_name_idx + 1]
                    + ["dedup_table"]
                    + [
                        col
                        for col in df.columns[table_name_idx + 1 :]
                        if col not in ["dedup_table", "primary_key", "notes"]
                    ]
                    + ["primary_key", "notes"]
                )

                # Add the new columns
                df = df.with_columns(
                    pl.Series("dedup_table", dedup_tables),
                    pl.Series("primary_key", primary_keys),
                    pl.Series("notes", notes),
                )

            else:  # source_table/target_table case
                primary_keys = []
                source_notes = []
                source_dedup_tables = []
                target_notes = []
                target_dedup_tables = []

                for source, target in zip(source_tables, target_tables):
                    # Use source table's primary key (should be same for both)
                    primary_key = None

                    # Process source
                    if source is None or str(source) == "null" or source not in results:
                        source_notes.append(
                            "No table specified"
                            if not source or str(source) == "null"
                            else None
                        )
                        source_dedup_tables.append(None)
                    else:
                        key, error, dedup_table, _ = results[source]
                        primary_key = key  # Use source's key as the primary key
                        source_notes.append(error)
                        source_dedup_tables.append(dedup_table)

                    # Process target
                    if target is None or str(target) == "null" or target not in results:
                        target_notes.append(
                            "No table specified"
                            if not target or str(target) == "null"
                            else None
                        )
                        target_dedup_tables.append(None)
                    else:
                        key, error, dedup_table, _ = results[target]
                        # If source didn't have a key, use target's key
                        if primary_key is None:
                            primary_key = key
                        target_notes.append(error)
                        target_dedup_tables.append(dedup_table)

                    primary_keys.append(primary_key)

                # Find positions of source and target table columns
                source_table_idx = df.columns.index("source_table")
                target_table_idx = df.columns.index("target_table")

                # Create new column order:
                # everything up to source_table, source_table, source_dedup_table,
                # everything between source and target, target_table, target_dedup_table,
                # rest of columns, then primary_key and notes
                new_columns = (
                    df.columns[: source_table_idx + 1]
                    + ["source_dedup_table"]
                    + [
                        col
                        for col in df.columns[
                            source_table_idx + 1 : target_table_idx + 1
                        ]
                        if col
                        not in [
                            "source_dedup_table",
                            "target_dedup_table",
                            "primary_key",
                            "source_notes",
                            "target_notes",
                        ]
                    ]
                    + ["target_dedup_table"]
                    + [
                        col
                        for col in df.columns[target_table_idx + 1 :]
                        if col
                        not in [
                            "source_dedup_table",
                            "target_dedup_table",
                            "primary_key",
                            "source_notes",
                            "target_notes",
                        ]
                    ]
                    + [
                        "primary_key",
                        "source_notes",
                        "target_notes",
                    ]
                )

                # Add the new columns
                df = df.with_columns(
                    pl.Series("source_dedup_table", source_dedup_tables),
                    pl.Series("target_dedup_table", target_dedup_tables),
                    pl.Series("primary_key", primary_keys),
                    pl.Series("source_notes", source_notes),
                    pl.Series("target_notes", target_notes),
                )

            # Reorder columns
            df = df.select(new_columns)

            # Sort the dataframe
            if table_column == "table_name":
                # For single table mode, sort by dedup_table and primary_key
                df = df.sort(["dedup_table", "primary_key"], nulls_last=True)
            else:
                # For source/target mode, sort by source_dedup_table and primary_key
                df = df.sort(["source_dedup_table", "primary_key"], nulls_last=True)

            # Write back to CSV without quotes for column names
            df.write_csv(tables_file, quote_style="necessary")
            logger.info(f"Updated primary keys in {tables_file}")


def main(args=None):
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
    parser.add_argument("--table", help="Single table name to analyze")
    parser.add_argument(
        "--tables-file",
        help="CSV file containing list of tables (with table_name or source_table/target_table columns). Can also be specified in config YAML.",
    )
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
    parser.add_argument(
        "--sample-size",
        type=int,
        help="Create a temporary sample table with this many records for analysis (improves performance on large tables)",
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

    # Note: We don't validate here because tables_file can come from config YAML
    if args.table and args.tables_file:
        parser.error("Cannot specify both --table and --tables-file")

    keyfinder(
        config_path=args.config,
        table_name=args.table,
        tables_file=args.tables_file,
        max_key_size=args.max_size,
        max_columns=args.max_columns,
        exclude_columns=args.exclude,
        include_columns=args.include_only,
        force=args.force,
        verbose=args.verbose,
        sample_size=args.sample_size,
    )


if __name__ == "__main__":
    main()
