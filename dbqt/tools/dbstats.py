"""Database statistics tool — row counts with optional column comparison."""

import polars as pl
import logging
import threading

from dbqt.tools.utils import (
    load_config,
    ConnectionPool,
    setup_logging,
    Timer,
    _read_table_lists,
    get_metadata_for_table,
    metadata_to_df,
    fetch_metadata_parallel,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _load_configs(config_path, source_config_path, target_config_path):
    """Return (source_config, target_config, tables_file, max_workers)."""
    if source_config_path and target_config_path:
        source_config = load_config(source_config_path)
        target_config = load_config(target_config_path)
        tables_file = source_config.get("tables_file") or target_config.get("tables_file")
        max_workers = source_config.get("max_workers", 4)
    elif config_path:
        config = load_config(config_path)
        source_config = target_config = config
        tables_file = config.get("tables_file")
        max_workers = config.get("max_workers", 4)
    else:
        raise ValueError(
            "Either config_path or both source/target config paths must be provided"
        )
    return source_config, target_config, tables_file, max_workers


# ---------------------------------------------------------------------------
# Row-count functionality
# ---------------------------------------------------------------------------


def get_row_count_for_table(connector, table_name, prefix=""):
    """Get row count for a single table using a shared connector."""
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
    config_path: str = None,
    source_config_path: str = None,
    target_config_path: str = None,
):
    """Collect row counts for tables listed in a CSV file."""
    with Timer("Database statistics collection"):
        source_config, target_config, tables_file, max_workers = _load_configs(
            config_path, source_config_path, target_config_path
        )
        df, source_tables, target_tables = _read_table_lists(
            tables_file, source_config, target_config
        )

        if target_tables is not None:
            # source/target mode

            # Determine which tables to actually count based on discovery status
            discovery_status = {}
            if "_discovery_status" in df.columns:
                for row in df.iter_rows(named=True):
                    status = row["_discovery_status"]
                    discovery_status[row["source_table"]] = status

            # Filter to tables that should actually be counted
            source_tables_to_count = [
                t for t in source_tables
                if discovery_status.get(t, "common") in ("common", "source_only_count")
                and discovery_status.get(t, "common") != "target_only"
                and discovery_status.get(t, "common") != "source_only"
            ]
            target_tables_to_count = [
                t for t in target_tables
                if discovery_status.get(t, "common") in ("common", "target_only_count")
                and discovery_status.get(t, "common") != "source_only"
                and discovery_status.get(t, "common") != "target_only"
            ]

            source_results = {}
            target_results = {}

            if source_tables_to_count:
                source_workers = min(max_workers, len(source_tables_to_count))
                with ConnectionPool(source_config, source_workers) as pool:
                    source_results = pool.execute_parallel(
                        lambda c, t: get_row_count_for_table(c, t, "source:"),
                        source_tables_to_count,
                    )

            if target_tables_to_count:
                target_workers = min(max_workers, len(target_tables_to_count))
                with ConnectionPool(target_config, target_workers) as pool:
                    target_results = pool.execute_parallel(
                        lambda c, t: get_row_count_for_table(c, t, "target:"),
                        target_tables_to_count,
                    )

            src_counts, src_notes, tgt_counts, tgt_notes = [], [], [], []
            for t in source_tables:
                status = discovery_status.get(t, "common")
                if status == "source_only":
                    src_counts.append(None)
                    src_notes.append("Only in source, row count skipped")
                elif status == "target_only":
                    src_counts.append(None)
                    src_notes.append("Only in target, row count skipped")
                else:
                    c, e = source_results[t]
                    src_counts.append(c)
                    src_notes.append(e)
            for t in target_tables:
                status = discovery_status.get(t, "common")
                if status == "target_only":
                    tgt_counts.append(None)
                    tgt_notes.append("Only in target, row count skipped")
                elif status == "source_only":
                    tgt_counts.append(None)
                    tgt_notes.append("Only in source, row count skipped")
                else:
                    c, e = target_results[t]
                    tgt_counts.append(c)
                    tgt_notes.append(e)

            df = df.with_columns(
                pl.Series("source_row_count", src_counts),
                pl.Series("source_notes", src_notes),
                pl.Series("target_row_count", tgt_counts),
                pl.Series("target_notes", tgt_notes),
            )

            # Reorder columns
            cols = df.columns
            for col_name, anchor, offset in [
                ("source_row_count", "source_table", 1),
                ("source_notes", "source_table", 2),
                ("target_row_count", "target_table", 1),
                ("target_notes", "target_table", 2),
            ]:
                cols.pop(cols.index(col_name))
                cols.insert(cols.index(anchor) + offset, col_name)
            df = df.select(cols)

            df = df.with_columns(
                (pl.col("target_row_count") - pl.col("source_row_count")).alias("difference")
            )
            df = df.with_columns(
                (((pl.col("difference") / pl.col("source_row_count")) * 100))
                .fill_nan(0.0)
                .alias("percentage_difference")
            )
        else:
            # single-table mode
            actual_workers = min(max_workers, len(source_tables))
            with ConnectionPool(source_config, actual_workers) as pool:
                results = pool.execute_parallel(get_row_count_for_table, source_tables)

            counts, notes = [], []
            for t in source_tables:
                c, e = results[t]
                counts.append(c)
                notes.append(e)

            df = df.with_columns(
                pl.Series("row_count", counts),
                pl.Series("notes", notes),
            )

        # Drop internal columns before writing
        if "_discovery_status" in df.columns:
            df = df.drop("_discovery_status")

        output_file = tables_file if tables_file else "dbqt_table_stats.csv"
        df.write_csv(output_file)
        logger.info(f"Updated row counts in {output_file}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(args=None):
    import argparse
    from dbqt.tools.colcompare import generate_config_file, colcompare_from_db, load_type_mappings

    parser = argparse.ArgumentParser(
        description="Database statistics: row counts and/or column comparison",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  rowcount   - Get row counts for tables (default)
  colcompare - Compare column schemas between source and target
  both       - Run row counts then column comparison

Examples:
  dbqt dbstats --config config.yaml
  dbqt dbstats colcompare --source-config src.yaml --target-config tgt.yaml
  dbqt dbstats both --source-config src.yaml --target-config tgt.yaml
        """,
    )
    parser.add_argument(
        "mode",
        nargs="?",
        default="rowcount",
        choices=["rowcount", "colcompare", "both"],
        help="Operation mode (default: rowcount)",
    )
    parser.add_argument("--config", help="YAML config (used as both source and target)")
    parser.add_argument("--source-config", help="YAML config for source database")
    parser.add_argument("--target-config", help="YAML config for target database")
    parser.add_argument("--type-config", help="Path to type mappings config file (colcompare mode)")
    parser.add_argument(
        "--generate-col-mappings",
        action="store_true",
        help="Generate a default column type mappings configuration file",
    )
    parser.add_argument(
        "--output", "-o",
        default="colcompare_config.yaml",
        help="Output path for generated config file",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    setup_logging(args.verbose)

    if args.generate_col_mappings:
        generate_config_file(args.output)
        return

    mode = args.mode

    if mode in ("rowcount", "both"):
        if args.source_config and args.target_config:
            get_table_stats(
                source_config_path=args.source_config,
                target_config_path=args.target_config,
            )
        elif args.config:
            get_table_stats(config_path=args.config)
        else:
            parser.error(
                "Either --config or both --source-config and --target-config required"
            )

    if mode in ("colcompare", "both"):
        type_mappings = load_type_mappings(args.type_config)
        if args.source_config and args.target_config:
            colcompare_from_db(args.source_config, args.target_config, type_mappings)
        elif args.config:
            colcompare_from_db(args.config, args.config, type_mappings)
        else:
            parser.error(
                "Either --config or both --source-config and --target-config required"
            )


if __name__ == "__main__":
    main()
