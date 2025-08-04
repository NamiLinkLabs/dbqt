"""
Check for columns where all records are null across a list of Snowflake tables.
"""

import argparse
import csv
import yaml
import logging
from dbqt.connections import create_connector

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def read_table_list(csv_path: str) -> list:
    """Read table names from CSV file."""
    tables = []
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        first_row = True
        for row in reader:
            if row and row[0].strip():  # Skip empty rows
                # Skip header row if first row is "table_name"
                if first_row and row[0].strip().lower() == 'table_name':
                    first_row = False
                    continue
                tables.append(row[0].strip())
                first_row = False
    return tables


def get_all_table_columns(connector, tables: list) -> dict:
    """Get all column names for all tables in a single query."""
    try:
        # Get database and schema from connector config
        database = connector.config['database']
        schema = connector.config['schema']
        
        # Create a single query to get all columns for all tables
        table_list = "', '".join(tables)
        query = f"""
        SELECT UPPER(TABLE_NAME) AS TABLE_NAME, UPPER(COLUMN_NAME) AS COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE 
        UPPER(TABLE_CATALOG) = UPPER('{database}') 
        AND UPPER(TABLE_SCHEMA) = UPPER('{schema}') 
        AND UPPER(TABLE_NAME) IN ('{table_list}')
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
        
        logger.info(f"Fetching metadata for {len(tables)} tables in single query")
        result = connector.run_query(query)
        
        # Organize results by table
        table_columns = {}
        if result and result.rows:
            for row in result.rows:  # Process all data rows
                table_name = row[0]
                column_name = row[1]
                if table_name not in table_columns:
                    table_columns[table_name] = []
                table_columns[table_name].append(column_name)
        
        return table_columns
        
    except Exception as e:
        logger.error(f"Error getting columns for all tables: {str(e)}")
        return {}


def check_null_columns(connector, table_name: str, columns: list) -> dict:
    """
    Check which columns in a table have all null values and get distinct value counts.

    Returns:
        dict: {column_name: {'is_all_null': bool, 'distinct_count': int}, ...}
    """
    results = {}

    if not columns:
        logger.warning(f"No columns found for table {table_name}")
        return results

    # Build a single query that counts distinct values for all columns
    # This is much more efficient than multiple queries
    distinct_checks = []
    for col in columns:
        distinct_checks.append(f"COUNT(DISTINCT {col}) AS {col}_distinct_count")

    query = f"""
    SELECT {', '.join(distinct_checks)}
    FROM {table_name}
    """

    try:
        result = connector.run_query(query)
        if result and result.rows:  # Check if we have data rows
            row = result.rows[0]  # First data row
            for i, col in enumerate(columns):
                distinct_count = int(row[i]) if row[i] else 0
                # If distinct count is 0, all values are null
                results[col] = {
                    'is_all_null': distinct_count == 0,
                    'distinct_count': distinct_count
                }

    except Exception as e:
        logger.error(f"Error checking null columns for {table_name}: {str(e)}")
        # If the optimized query fails, fall back to a simpler approach
        try:
            # Alternative: use SUM to count non-null values
            sum_checks = []
            for col in columns:
                sum_checks.append(f"SUM(CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END) AS {col}_not_null_count")
            
            fallback_query = f"""
            SELECT {', '.join(sum_checks)}
            FROM {table_name}
            """
            
            result = connector.run_query(fallback_query)
            if result and result.rows:
                row = result.rows[0]
                for i, col in enumerate(columns):
                    not_null_count = int(row[i]) if row[i] else 0
                    results[col] = {
                        'is_all_null': not_null_count == 0,
                        'distinct_count': 0 if not_null_count == 0 else None  # Can't determine exact distinct count with fallback
                    }
            
        except Exception as fallback_e:
            logger.error(f"Fallback query also failed for {table_name}: {str(fallback_e)}")
            # Mark all columns as unknown
            for col in columns:
                results[col] = {'is_all_null': None, 'distinct_count': None}

    return results


def check_tables_for_null_columns(connector, tables: list, output_file: str) -> None:
    """
    Check all tables for columns with all null values.

    Args:
        connector: Database connector instance
        tables: List of table names to check
        output_file: Path to output file
    """
    logger.info(f"Checking {len(tables)} tables for null columns")

    # Get all column metadata in a single query
    all_table_columns = get_all_table_columns(connector, tables)
    
    all_null_columns = []

    with open(output_file, 'w') as f:
        f.write("# Null Column Check Results\n")
        f.write("# " + "=" * 50 + "\n\n")

        for i, table_name in enumerate(tables, 1):
            logger.info(f"Processing table {i}/{len(tables)}: {table_name}")

            try:
                # Get columns for this table from pre-fetched metadata
                columns = all_table_columns.get(table_name.upper(), [])

                if not columns:
                    f.write(f"## {table_name}\n")
                    f.write("ERROR: Could not retrieve columns or table not found\n\n")
                    continue

                # Check for null columns
                null_results = check_null_columns(connector, table_name, columns)

                f.write(f"## {table_name}\n")
                f.write(f"Total columns: {len(columns)}\n")

                table_null_columns = []
                for col, result in null_results.items():
                    if result.get('is_all_null') is True:
                        table_null_columns.append(col)
                        all_null_columns.append(f"{table_name}.{col}")

                if table_null_columns:
                    f.write(f"**Columns with all NULL values ({len(table_null_columns)}):**\n")
                    for col in table_null_columns:
                        f.write(f"  - {col}\n")
                else:
                    f.write("No columns with all NULL values found.\n")

                # Show distinct value counts for all columns
                f.write(f"\n**Column distinct value counts:**\n")
                # Sort columns by distinct count for better readability
                sorted_columns = sorted(
                    null_results.items(), 
                    key=lambda x: x[1].get('distinct_count', -1) if x[1].get('distinct_count') is not None else -1
                )
                
                for col, result in sorted_columns:
                    distinct_count = result.get('distinct_count')
                    if distinct_count is not None:
                        if distinct_count == 0:
                            f.write(f"  - {col}: **0** (all NULL)\n")
                        elif distinct_count == 1:
                            f.write(f"  - {col}: **1** (single value)\n")
                        else:
                            f.write(f"  - {col}: {distinct_count}\n")
                    else:
                        f.write(f"  - {col}: *unknown* (check error)\n")

                # Show any errors
                error_columns = [col for col, result in null_results.items() if result.get('is_all_null') is None]
                if error_columns:
                    f.write(f"\n**Columns with check errors ({len(error_columns)}):**\n")
                    for col in error_columns:
                        f.write(f"  - {col}\n")

                f.write("\n")
                # Count columns by distinct value ranges for logging
                distinct_counts = [result.get('distinct_count', 0) for result in null_results.values() if result.get('distinct_count') is not None]
                zero_distinct = sum(1 for count in distinct_counts if count == 0)
                one_distinct = sum(1 for count in distinct_counts if count == 1)
                
                logger.info(f"Completed {table_name}: {len(table_null_columns)} null columns, {one_distinct} single-value columns")

            except Exception as e:
                logger.error(f"Error processing table {table_name}: {str(e)}")
                f.write(f"## {table_name}\n")
                f.write(f"ERROR: {str(e)}\n\n")

        # Summary
        f.write("# Summary\n")
        f.write(f"Total tables checked: {len(tables)}\n")
        f.write(f"Total columns with all NULL values: {len(all_null_columns)}\n\n")

        if all_null_columns:
            f.write("## All NULL Columns Found:\n")
            for col in all_null_columns:
                f.write(f"  - {col}\n")

    logger.info(f"Null column check completed. Results written to {output_file}")
    logger.info(f"Found {len(all_null_columns)} columns with all NULL values")


def main(args=None):
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check for columns where all records are null across Snowflake tables"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to Snowflake configuration YAML file (should include tables_file)"
    )
    parser.add_argument(
        "--tables",
        help="Path to CSV file containing table names (overrides tables_file in config)"
    )
    parser.add_argument(
        "--output",
        default="null_columns_report.md",
        help="Output file path (default: null_columns_report.md)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    parsed_args = parser.parse_args(args)

    # Setup logging
    log_level = logging.DEBUG if parsed_args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        # Load configuration
        config = load_config(parsed_args.config)

        # Ensure it's a Snowflake configuration
        if config.get('connection', {}).get('type') != 'Snowflake':
            raise ValueError("Configuration must be for Snowflake connector")

        # Determine tables file path
        tables_file = parsed_args.tables or config.get('tables_file')
        if not tables_file:
            raise ValueError("No tables file specified. Use --tables argument or add tables_file to config")

        # Read table names from CSV
        tables = read_table_list(tables_file)
        if not tables:
            raise ValueError(f"No table names found in CSV file: {tables_file}")

        logger.info(f"Found {len(tables)} tables to check from {tables_file}")

        # Create connector and connect
        connector = create_connector(config['connection'])
        connector.connect()

        try:
            # Check tables for null columns
            check_tables_for_null_columns(connector, tables, parsed_args.output)
        finally:
            connector.disconnect()

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
