"""Check for columns where all records are null across Snowflake tables."""

import argparse
import csv
import yaml
import logging
from dbqt.connections import create_connector

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def read_table_list(csv_path: str) -> list:
    tables = []
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if row and row[0].strip():
                # Skip header if first row is "table_name"
                if i == 0 and row[0].strip().lower() == 'table_name':
                    continue
                tables.append(row[0].strip())
    return tables


def get_table_columns(connector, tables: list) -> dict:
    database = connector.config['database']
    schema = connector.config['schema']
    table_list = "', '".join(tables)
    
    query = f"""
    SELECT UPPER(TABLE_NAME), UPPER(COLUMN_NAME)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE UPPER(TABLE_CATALOG) = UPPER('{database}') 
    AND UPPER(TABLE_SCHEMA) = UPPER('{schema}') 
    AND UPPER(TABLE_NAME) IN ('{table_list}')
    ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
    
    result = connector.run_query(query)
    table_columns = {}
    
    if result and result.rows:
        for table_name, column_name in result.rows:
            if table_name not in table_columns:
                table_columns[table_name] = []
            table_columns[table_name].append(column_name)
    
    return table_columns


def check_null_columns(connector, table_name: str, columns: list) -> dict:
    if not columns:
        return {}
    
    # Count distinct values for all columns in one query
    distinct_checks = [f"COUNT(DISTINCT {col}) AS {col}_count" for col in columns]
    query = f"SELECT {', '.join(distinct_checks)} FROM {table_name}"
    
    try:
        result = connector.run_query(query)
        if result and result.rows:
            row = result.rows[0]
            return {col: int(row[i]) if row[i] else 0 for i, col in enumerate(columns)}
    except Exception as e:
        logger.error(f"Error checking {table_name}: {e}")
    
    return {}


def write_results(output_file: str, results: dict):
    with open(output_file, 'w') as f:
        f.write("# Null Column Check Results\n\n")
        
        all_null_columns = []
        
        for table_name, columns in results.items():
            if not columns:
                f.write(f"## {table_name}\nERROR: No columns found\n\n")
                continue
                
            null_cols = [col for col, count in columns.items() if count == 0]
            all_null_columns.extend(f"{table_name}.{col}" for col in null_cols)
            
            f.write(f"## {table_name}\n")
            f.write(f"Total columns: {len(columns)}\n")
            
            if null_cols:
                f.write(f"NULL columns ({len(null_cols)}): {', '.join(null_cols)}\n")
            else:
                f.write("No NULL columns found\n")
            
            # Show columns with low distinct counts
            low_distinct = [(col, count) for col, count in columns.items() if 0 < count <= 5]
            if low_distinct:
                f.write(f"Low distinct counts: {dict(low_distinct)}\n")
            f.write("\n")
        
        f.write(f"# Summary\nTotal NULL columns: {len(all_null_columns)}\n")
        if all_null_columns:
            f.write(f"NULL columns: {', '.join(all_null_columns)}\n")


def main(args=None):
    parser = argparse.ArgumentParser(description="Check for NULL columns in Snowflake tables")
    parser.add_argument("--config", required=True, help="Snowflake config YAML file")
    parser.add_argument("--tables", help="CSV file with table names")
    parser.add_argument("--output", default="null_columns_report.md", help="Output file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    parsed_args = parser.parse_args(args)

    logging.basicConfig(
        level=logging.DEBUG if parsed_args.verbose else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        config = load_config(parsed_args.config)
        
        if config.get('connection', {}).get('type') != 'Snowflake':
            raise ValueError("Must use Snowflake connector")

        tables_file = parsed_args.tables or config.get('tables_file')
        if not tables_file:
            raise ValueError("No tables file specified")

        tables = read_table_list(tables_file)
        if not tables:
            raise ValueError(f"No tables found in {tables_file}")

        logger.info(f"Checking {len(tables)} tables")

        connector = create_connector(config['connection'])
        connector.connect()

        try:
            all_table_columns = get_table_columns(connector, tables)
            results = {}
            
            for table_name in tables:
                columns = all_table_columns.get(table_name.upper(), [])
                if columns:
                    results[table_name] = check_null_columns(connector, table_name, columns)
                else:
                    results[table_name] = {}
                    
            write_results(parsed_args.output, results)
            logger.info(f"Results written to {parsed_args.output}")
            
        finally:
            connector.disconnect()

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
