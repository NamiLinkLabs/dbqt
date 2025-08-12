#!/usr/bin/env python3
"""
Run dynamic queries against Athena using variables from a CSV file.
"""

import argparse
import csv
import yaml
import logging
from dbqt.connections import create_connector

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def read_csv_values(csv_path: str) -> list:
    """Read values from CSV file."""
    values = []
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip():  # Skip empty rows
                values.append(row[0].strip())
    return values


def run_dynamic_queries(
    connector, csv_values: list, query_template: str, output_file: str
) -> None:
    """
    Run dynamic queries using variables from CSV file.

    Args:
        connector: Database connector instance
        csv_values: List of values from CSV to substitute in query
        query_template: Query template with {var_from_csv} placeholder
        output_file: Path to output file
    """
    logger.info(
        f"Running dynamic queries for {len(csv_values)} values to {output_file}"
    )

    with open(output_file, "w") as f:
        f.write("-- Generated query results\n")
        f.write("-- " + "=" * 50 + "\n\n")

        for i, csv_value in enumerate(csv_values, 1):
            logger.info(f"Processing value {i}/{len(csv_values)}: {csv_value}")

            try:
                # Substitute the CSV value into the query template
                query = query_template.format(var_from_csv=csv_value)
                result = connector.run_query(query)

                f.write(f"-- Query for: {csv_value}\n")
                f.write(f"-- {query}\n")

                if result and len(result) > 0:
                    # Format the results
                    for row in result:
                        f.write(f"{', '.join(str(col) for col in row)}\n")
                    logger.info(f"Successfully executed query for {csv_value}")
                else:
                    f.write("-- No results returned\n")
                    logger.warning(f"No results returned for {csv_value}")

                f.write("\n")

            except Exception as e:
                logger.error(f"Error executing query for {csv_value}: {str(e)}")
                f.write(
                    f"-- ERROR: Failed to execute query for {csv_value}: {str(e)}\n\n"
                )

    logger.info(f"Query execution completed. Output written to {output_file}")


def main(args=None):
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run dynamic queries against Athena using variables from CSV file"
    )
    parser.add_argument(
        "--config", required=True, help="Path to Athena configuration YAML file"
    )
    parser.add_argument(
        "--csv", required=True, help="Path to CSV file containing values (one per row)"
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Query template with {var_from_csv} placeholder (e.g., 'SELECT COUNT(1) FROM {var_from_csv}')",
    )
    parser.add_argument(
        "--output",
        default="query_results.txt",
        help="Output file path (default: query_results.txt)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    parsed_args = parser.parse_args(args)

    # Setup logging
    log_level = logging.DEBUG if parsed_args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    try:
        # Load configuration
        config = load_config(parsed_args.config)

        # Ensure it's an Athena configuration
        if config.get("connection", {}).get("type") != "Athena":
            raise ValueError("Configuration must be for Athena connector")

        # Read values from CSV
        csv_values = read_csv_values(parsed_args.csv)
        if not csv_values:
            raise ValueError("No values found in CSV file")

        logger.info(f"Found {len(csv_values)} values to process")

        # Create connector and connect
        connector = create_connector(config["connection"])
        connector.connect()

        try:
            # Run dynamic queries
            run_dynamic_queries(
                connector, csv_values, parsed_args.query, parsed_args.output
            )
        finally:
            connector.disconnect()

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
