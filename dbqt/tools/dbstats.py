"""Database statistics tool — row counts with optional column comparison."""

import logging
import os
import threading

import polars as pl

from dbqt.tools.utils import (
    load_config,
    ConnectionPool,
    setup_logging,
    Timer,
    HTMLReport,
    _read_table_lists,
    get_schema_label,
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
        tables_file = source_config.get("tables_file") or target_config.get(
            "tables_file"
        )
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
    *,
    report: HTMLReport = None,
    source_config: dict = None,
    target_config: dict = None,
    table_lists: tuple = None,
):
    """Collect row counts for tables listed in a CSV file.

    Parameters
    ----------
    report : HTMLReport | None
        If provided, the row-count tab is added to this existing report
        instead of creating a standalone file.  The caller is responsible
        for saving the report.  Returns ``None`` in that case.
    source_config / target_config : dict | None
        Pre-loaded config dicts.  When provided the corresponding config
        paths are not read, avoiding redundant file I/O.
    table_lists : tuple | None
        Pre-computed ``(df, source_tables, target_tables)`` from
        ``_read_table_lists``.  When provided the table-discovery step is
        skipped entirely, avoiding redundant database connections.
    """
    with Timer("Database statistics collection"):
        if source_config is None or target_config is None:
            source_config, target_config, tables_file, max_workers = _load_configs(
                config_path, source_config_path, target_config_path
            )
        else:
            tables_file = source_config.get("tables_file") or target_config.get(
                "tables_file"
            )
            max_workers = source_config.get("max_workers", 4)

        if table_lists is not None:
            df, source_tables, target_tables = table_lists
        else:
            # Open a minimal pool (1 connection per side) to reuse for table
            # discovery, avoiding a second SSO browser prompt on Snowflake.
            actual_workers = min(max_workers, 1)
            with ConnectionPool(source_config, actual_workers) as src_pool:
                src_conn = src_pool.connectors[0]
                if target_config is not source_config:
                    with ConnectionPool(target_config, actual_workers) as tgt_pool:
                        tgt_conn = tgt_pool.connectors[0]
                        df, source_tables, target_tables = _read_table_lists(
                            tables_file,
                            source_config,
                            target_config,
                            source_connector=src_conn,
                            target_connector=tgt_conn,
                        )
                else:
                    df, source_tables, target_tables = _read_table_lists(
                        tables_file,
                        source_config,
                        target_config,
                        source_connector=src_conn,
                        target_connector=src_conn,
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
                t
                for t in source_tables
                if discovery_status.get(t, "common") in ("common", "source_only_count")
                and discovery_status.get(t, "common") != "target_only"
                and discovery_status.get(t, "common") != "source_only"
            ]
            target_tables_to_count = [
                t
                for t in target_tables
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

            # Reorder columns so counts appear next to their table columns
            cols = df.columns
            for col_name, anchor, offset in [
                ("source_row_count", "source_table", 1),
                ("source_notes", "source_table", 2),
                ("target_row_count", "target_table", 1),
                ("target_notes", "target_table", 2),
            ]:
                if col_name in cols and anchor in cols:
                    cols.pop(cols.index(col_name))
                    cols.insert(cols.index(anchor) + offset, col_name)
            df = df.select(cols)

            df = df.with_columns(
                (pl.col("target_row_count") - pl.col("source_row_count")).alias(
                    "difference"
                )
            )
            df = df.with_columns(
                (
                    (((pl.col("difference") / pl.col("source_row_count")) * 100))
                    .fill_nan(0.0)
                    .round(2)
                    .cast(pl.Utf8)
                    + pl.lit("%")
                ).alias("percentage_difference")
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

        # If an external report was provided, add the tab and return
        if report is not None:
            report.add_polars_tab("Row Counts", df)
            logger.info("Row counts added to report")
            return None

        # Standalone mode — write our own HTML file
        os.makedirs("results", exist_ok=True)
        if tables_file:
            base = os.path.splitext(os.path.basename(tables_file))[0]
        else:
            base = get_schema_label(target_config)
        output_file = os.path.join("results", f"{base}_dbstats.html")

        standalone = HTMLReport(f"Database Statistics — {base}")
        standalone.add_polars_tab("Row Counts", df)
        standalone.save(output_file)
        logger.info(f"Report saved to {output_file}")
        return output_file


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(args=None):
    import argparse
    from dbqt.tools.colcompare import (
        generate_config_file,
        colcompare_from_db,
        load_type_mappings,
        collect_excluded_cols,
    )

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
  dbqt dbstats colcompare --type-config colcompare_config.yaml --source-config src.yaml --target-config tgt.yaml
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
    parser.add_argument(
        "--type-config", help="Path to type mappings config file (colcompare mode)"
    )
    parser.add_argument(
        "--generate-col-mappings",
        action="store_true",
        help="Generate a default column type mappings configuration file",
    )
    parser.add_argument(
        "--output",
        "-o",
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

    # Determine config sources
    has_dual = args.source_config and args.target_config
    has_single = args.config
    if not has_dual and not has_single:
        parser.error(
            "Either --config or both --source-config and --target-config required"
        )

    if mode == "both":
        # Build a single combined HTML report.
        # Load configs and discover tables ONCE so that both row-count
        # and column-comparison steps reuse them without reconnecting.
        from datetime import datetime

        if has_dual:
            source_config = load_config(args.source_config)
            target_config = load_config(args.target_config)
        else:
            source_config = target_config = load_config(args.config)

        tables_file = source_config.get("tables_file") or target_config.get(
            "tables_file"
        )
        if tables_file:
            base = os.path.splitext(os.path.basename(tables_file))[0]
        else:
            base = get_schema_label(target_config)

        # Discover tables once
        table_lists = _read_table_lists(tables_file, source_config, target_config)
        _df, source_tables, target_tables = table_lists

        report = HTMLReport(f"Database Statistics — {base}")

        # Row counts tab — pass pre-loaded configs and table lists
        get_table_stats(
            report=report,
            source_config=source_config,
            target_config=target_config,
            table_lists=table_lists,
        )

        # Column comparison tabs — pass pre-loaded configs and table lists.
        # Connectors are created inside colcompare_from_db (metadata fetch
        # needs fresh connections after the row-count pool has shut down).
        type_mappings = load_type_mappings(args.type_config)
        excluded_cols = collect_excluded_cols(
            args.type_config, source_config, target_config
        )
        colcompare_from_db(
            type_mappings=type_mappings,
            excluded_cols=excluded_cols,
            report=report,
            source_config=source_config,
            target_config=target_config,
            source_tables=source_tables,
            target_tables=target_tables,
            tables_file=tables_file,
        )

        # Save combined report
        os.makedirs("results", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join("results", f"{base}_dbstats_{ts}.html")
        report.save(output_path)

    elif mode == "rowcount":
        if has_dual:
            get_table_stats(
                source_config_path=args.source_config,
                target_config_path=args.target_config,
            )
        else:
            get_table_stats(config_path=args.config)

    elif mode == "colcompare":
        type_mappings = load_type_mappings(args.type_config)
        src_cfg_path = args.source_config or args.config
        tgt_cfg_path = args.target_config or args.config
        excluded_cols = collect_excluded_cols(
            args.type_config,
            load_config(src_cfg_path),
            load_config(tgt_cfg_path),
        )
        colcompare_from_db(src_cfg_path, tgt_cfg_path, type_mappings, excluded_cols)


if __name__ == "__main__":
    main()
