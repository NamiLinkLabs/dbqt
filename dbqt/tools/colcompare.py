"""Column comparison tool — compare schemas between files or databases."""

import argparse
import logging
import re
from datetime import datetime
from pathlib import Path

import polars as pl
import pyarrow
import pyarrow.parquet as pq
import yaml

from dbqt.tools.utils import (
    load_config,
    setup_logging,
    Timer,
    HTMLReport,
    fetch_all_metadata_as_df,
    _read_table_lists,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type-mapping helpers
# ---------------------------------------------------------------------------

DEFAULT_TYPE_MAPPINGS = {
    "INTEGER": ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "NUMBER"],
    "VARCHAR": ["VARCHAR", "TEXT", "CHAR", "STRING", "NVARCHAR", "VARCHAR2", "ENUM"],
    "DECIMAL": ["DECIMAL", "NUMERIC", "NUMBER"],
    "FLOAT": ["FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION"],
    "TIMESTAMP": ["TIMESTAMP", "DATETIME", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"],
    "DATE": ["DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"],
    "DATETIME": ["TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"],
    "BOOLEAN": ["BOOLEAN", "BOOL", "BIT"],
    "ENUM": ["TEXT"],
}


def load_type_mappings(config_path=None):
    """Load type mappings from YAML config file or return defaults."""
    if config_path and Path(config_path).exists():
        logger.info(f"Loading type mappings from {config_path}")
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            return config.get("type_mappings", DEFAULT_TYPE_MAPPINGS)
    return DEFAULT_TYPE_MAPPINGS


def load_excluded_cols(config_path=None):
    """Load excluded column names from YAML config file.

    Returns a set of upper-cased column names that should be ignored
    during column comparison.
    """
    if config_path and Path(config_path).exists():
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            raw = config.get("excluded_cols", [])
            if raw:
                excluded = {c.upper() for c in raw}
                logger.info(
                    f"Excluding {len(excluded)} columns from comparison: {excluded}"
                )
                return excluded
    return set()


def collect_excluded_cols(type_config=None, *configs):
    """Merge ``excluded_cols`` from a type-mappings config and any number of
    connection YAML configs (source / target).

    Each config can be a file path (``str``) or an already-loaded ``dict``.
    Returns a unified ``set`` of upper-cased column names.
    """
    merged: set[str] = set()

    # Type-mappings config (the dedicated colcompare config file)
    merged |= load_excluded_cols(type_config)

    # Connection configs (source / target YAML dicts or paths)
    for cfg in configs:
        if cfg is None:
            continue
        if isinstance(cfg, str):
            cfg = load_config(cfg)
        raw = cfg.get("excluded_cols", [])
        if raw:
            merged |= {c.upper() for c in raw}

    if merged:
        logger.info(f"Total excluded columns (merged): {merged}")
    return merged


def generate_config_file(output_path="colcompare_config.yaml"):
    """Generate a default configuration file with type mappings."""
    config = {"type_mappings": DEFAULT_TYPE_MAPPINGS}

    output_file = Path(output_path)
    if output_file.exists():
        logger.warning(f"Config file already exists at {output_path}")
        response = input("Overwrite? (y/n): ")
        if response.lower() != "y":
            logger.info("Config generation cancelled")
            return

    with open(output_path, "w") as f:
        f.write(
            "# Column comparison type mappings configuration.\n"
            "# Each key represents a type group, and the list contains equivalent types.\n"
        )
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        # Append excluded_cols in block style with commented examples
        f.write(
            "\n# Column names to exclude from comparison (case-insensitive)\n"
            "excluded_cols:\n"
            "  # - CREATED_AT\n"
            "  # - UPDATED_AT\n"
            "\n# Table name patterns to exclude (SQL-like % wildcards, case-insensitive)\n"
            "# These are applied in the connection YAML, not here. Example:\n"
            "# excluded_tables:\n"
            "#   - %_FINAL\n"
            "#   - TMP_%\n"
        )

    logger.info(f"Generated default config file at {output_path}")
    print(f"\nDefault configuration saved to: {output_path}")
    print("You can now edit this file to customize type mappings.")


def are_types_compatible(type1, type2, type_mappings=None):
    """Check if two data types are considered compatible."""
    if type_mappings is None:
        type_mappings = DEFAULT_TYPE_MAPPINGS

    type1, type2 = type1.upper(), type2.upper()
    type1 = type1.split("(")[0].strip()
    type2 = type2.split("(")[0].strip()

    if type1 == type2:
        return True

    if re.match(r"^TIMESTAMP.*", type1) and re.match(r"^TIMESTAMP.*", type2):
        return True

    for type_group in type_mappings.values():
        if type1 in type_group and type2 in type_group:
            return True

    return False


# ---------------------------------------------------------------------------
# Parquet / file-based schema helpers
# ---------------------------------------------------------------------------


def _process_nested_type(field_type, parent_name="", processed_fields=None):
    """Recursively process nested types in Parquet schema."""
    if processed_fields is None:
        processed_fields = []

    if isinstance(field_type, (pyarrow.lib.ListType, pyarrow.lib.LargeListType)):
        element_type = field_type.value_type
        if isinstance(element_type, pyarrow.lib.StructType):
            for nested_field in element_type:
                full_name = (
                    f"{parent_name}__{nested_field.name}"
                    if parent_name
                    else nested_field.name
                )
                if isinstance(
                    nested_field.type,
                    (
                        pyarrow.lib.ListType,
                        pyarrow.lib.LargeListType,
                        pyarrow.lib.StructType,
                        pyarrow.lib.MapType,
                    ),
                ):
                    _process_nested_type(nested_field.type, full_name, processed_fields)
                else:
                    processed_fields.append(
                        {"col_name": full_name, "type": str(nested_field.type)}
                    )
        else:
            processed_fields.append({"col_name": parent_name, "type": str(field_type)})

    elif isinstance(field_type, pyarrow.lib.StructType):
        for nested_field in field_type:
            full_name = (
                f"{parent_name}__{nested_field.name}"
                if parent_name
                else nested_field.name
            )
            if isinstance(
                nested_field.type,
                (
                    pyarrow.lib.ListType,
                    pyarrow.lib.LargeListType,
                    pyarrow.lib.StructType,
                    pyarrow.lib.MapType,
                ),
            ):
                _process_nested_type(nested_field.type, full_name, processed_fields)
            else:
                processed_fields.append(
                    {"col_name": full_name, "type": str(nested_field.type)}
                )

    elif isinstance(field_type, pyarrow.lib.MapType):
        processed_fields.append({"col_name": parent_name, "type": str(field_type)})

    return processed_fields


def _schema_to_df(schema, label="pq"):
    """Convert a pyarrow schema to a Polars DataFrame with SCH_TABLE/COL_NAME/DATA_TYPE."""
    fields = []
    for field in schema:
        if isinstance(
            field.type,
            (
                pyarrow.lib.ListType,
                pyarrow.lib.LargeListType,
                pyarrow.lib.StructType,
                pyarrow.lib.MapType,
            ),
        ):
            fields.extend(_process_nested_type(field.type, field.name))
        else:
            fields.append({"col_name": field.name, "type": str(field.type)})

    return pl.DataFrame(
        {
            "SCH_TABLE": [label] * len(fields),
            "COL_NAME": [f["col_name"] for f in fields],
            "DATA_TYPE": [f["type"] for f in fields],
        }
    )


def compare_and_unnest_parquet_schema(source_path, target_path):
    """Compare schemas of two Parquet files without loading full dataset."""
    return (
        _schema_to_df(pq.read_schema(source_path)),
        _schema_to_df(pq.read_schema(target_path)),
    )


def read_files(source_path, target_path):
    """Read source and target files using Polars."""
    if source_path.endswith(".parquet") and target_path.endswith(".parquet"):
        return compare_and_unnest_parquet_schema(source_path, target_path)

    source_df = pl.read_csv(source_path)
    target_df = pl.read_csv(target_path)

    for df_ref, name in [(source_df, "source"), (target_df, "target")]:
        if "DATA_TYPE" not in df_ref.columns:
            df_ref = df_ref.with_columns(pl.lit("N/A").alias("DATA_TYPE"))
        if name == "source":
            source_df = df_ref
        else:
            target_df = df_ref

    for df_ref, name in [(source_df, "source"), (target_df, "target")]:
        if "SCH" in df_ref.columns:
            df_ref = df_ref.with_columns(
                pl.concat_str([pl.col("SCH"), pl.lit("."), pl.col("TABLE_NAME")]).alias(
                    "SCH_TABLE"
                )
            )
        else:
            df_ref = df_ref.with_columns(pl.col("TABLE_NAME").alias("SCH_TABLE"))
        if name == "source":
            source_df = df_ref
        else:
            target_df = df_ref

    return source_df, target_df


# ---------------------------------------------------------------------------
# Comparison logic
# ---------------------------------------------------------------------------


def compare_tables(source_df, target_df):
    """Compare tables between source and target."""
    source_tables = set(source_df["SCH_TABLE"].unique())
    target_tables = set(target_df["SCH_TABLE"].unique())
    return {
        "common": sorted(source_tables & target_tables),
        "source_only": sorted(source_tables - target_tables),
        "target_only": sorted(target_tables - source_tables),
    }


def compare_columns(
    source_df, target_df, table_name, type_mappings=None, excluded_cols=None
):
    """Compare columns for a specific table.

    Parameters
    ----------
    excluded_cols : set[str] | None
        Upper-cased column names to ignore during comparison.
    """
    source_cols = source_df.filter(pl.col("SCH_TABLE") == table_name).select(
        ["COL_NAME", "DATA_TYPE"]
    )
    target_cols = target_df.filter(pl.col("SCH_TABLE") == table_name).select(
        ["COL_NAME", "DATA_TYPE"]
    )

    # Apply exclusion filter
    if excluded_cols:
        source_cols = source_cols.filter(~pl.col("COL_NAME").is_in(list(excluded_cols)))
        target_cols = target_cols.filter(~pl.col("COL_NAME").is_in(list(excluded_cols)))

    src_set = set(source_cols["COL_NAME"].to_list())
    tgt_set = set(target_cols["COL_NAME"].to_list())
    common = src_set & tgt_set

    mismatches = []
    for col in common:
        st = source_cols.filter(pl.col("COL_NAME") == col)["DATA_TYPE"].item()
        tt = target_cols.filter(pl.col("COL_NAME") == col)["DATA_TYPE"].item()
        if not are_types_compatible(st, tt, type_mappings):
            mismatches.append({"column": col, "source_type": st, "target_type": tt})

    return {
        "common": sorted(common),
        "source_only": sorted(src_set - tgt_set),
        "target_only": sorted(tgt_set - src_set),
        "datatype_mismatches": mismatches,
    }


# ---------------------------------------------------------------------------
# HTML report helpers
# ---------------------------------------------------------------------------


def build_colcompare_tabs(
    comparison_results,
    source_df,
    target_df,
    type_mappings=None,
    excluded_cols=None,
):
    """Return a list of (tab_name, columns, data) tuples for the comparison.

    This is decoupled from file I/O so that dbstats can merge tabs into a
    single report when running in *both* mode.
    """
    tabs = []

    # --- Table Comparison ---
    table_rows = []
    for cat in ("common", "source_only", "target_only"):
        label = cat.replace("_", " ").title()
        for t in comparison_results["tables"][cat]:
            table_rows.append({"Category": label, "Table Name": t})
    tabs.append(
        (
            "Table Comparison",
            HTMLReport.columns_from_names(["Category", "Table Name"]),
            table_rows,
        )
    )

    # --- Column Comparison ---
    col_rows = []
    for tbl in comparison_results["columns"]:
        tn = tbl["table_name"]
        src = dict(
            zip(
                source_df.filter(pl.col("SCH_TABLE") == tn)["COL_NAME"],
                source_df.filter(pl.col("SCH_TABLE") == tn)["DATA_TYPE"],
            )
        )
        tgt = dict(
            zip(
                target_df.filter(pl.col("SCH_TABLE") == tn)["COL_NAME"],
                target_df.filter(pl.col("SCH_TABLE") == tn)["DATA_TYPE"],
            )
        )
        for col in sorted(set(list(src) + list(tgt))):
            if excluded_cols and col.upper() in excluded_cols:
                continue
            st, tt = src.get(col, "N/A"), tgt.get(col, "N/A")
            if col in tbl["source_only"]:
                status = "Source Only"
            elif col in tbl["target_only"]:
                status = "Target Only"
            elif are_types_compatible(st, tt, type_mappings):
                status = "Matching"
            else:
                status = "Different Types"
            col_rows.append(
                {
                    "Table Name": tn,
                    "Column Name": col,
                    "Status": status,
                    "Source Type": st,
                    "Target Type": tt,
                }
            )
    tabs.append(
        (
            "Column Comparison",
            HTMLReport.columns_from_names(
                ["Table Name", "Column Name", "Status", "Source Type", "Target Type"]
            ),
            col_rows,
        )
    )

    # --- Datatype Mismatches ---
    mismatch_rows = []
    for tbl in comparison_results["columns"]:
        for m in tbl["datatype_mismatches"]:
            mismatch_rows.append(
                {
                    "Table Name": tbl["table_name"],
                    "Column Name": m["column"],
                    "Source Type": m["source_type"],
                    "Target Type": m["target_type"],
                }
            )
    tabs.append(
        (
            "Datatype Mismatches",
            HTMLReport.columns_from_names(
                ["Table Name", "Column Name", "Source Type", "Target Type"]
            ),
            mismatch_rows,
        )
    )

    return tabs


def create_html_report(
    comparison_results,
    source_df,
    target_df,
    file_name,
    type_mappings=None,
    excluded_cols=None,
):
    """Create an interactive HTML report with Tabulator tables."""
    tabs = build_colcompare_tabs(
        comparison_results, source_df, target_df, type_mappings, excluded_cols
    )

    safe_name = file_name.split("/")[-1] if "/" in file_name else file_name
    report = HTMLReport(f"Column Comparison — {safe_name}")
    for tab_name, columns, data in tabs:
        report.add_tab(tab_name, columns=columns, data=data)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"results/{safe_name}_{ts}.html"
    return report.save(output_path)


# ---------------------------------------------------------------------------
# Core comparison runners
# ---------------------------------------------------------------------------


def _run_comparison(
    source_df, target_df, report_name, type_mappings=None, excluded_cols=None
):
    """Run table/column comparison and generate HTML report."""
    table_comparison = compare_tables(source_df, target_df)
    column_comparisons = []
    for tbl in table_comparison["common"]:
        cc = compare_columns(source_df, target_df, tbl, type_mappings, excluded_cols)
        column_comparisons.append({"table_name": tbl, **cc})

    results = {"tables": table_comparison, "columns": column_comparisons}
    output_path = create_html_report(
        results, source_df, target_df, report_name, type_mappings, excluded_cols
    )
    results["output_path"] = output_path
    return results


def colcompare_from_db(
    source_config_path=None,
    target_config_path=None,
    type_mappings=None,
    excluded_cols=None,
    *,
    report=None,
    source_config=None,
    target_config=None,
    source_tables=None,
    target_tables=None,
    tables_file=None,
    source_connector=None,
    target_connector=None,
):
    """Compare column schemas by fetching metadata directly from databases.

    Parameters
    ----------
    source_config_path / target_config_path : str | None
        Paths to YAML config files.  Ignored when *source_config* /
        *target_config* are supplied directly.
    report : HTMLReport | None
        If provided, colcompare tabs are added to this existing report
        instead of creating a standalone file.  Returns ``None`` in that
        case (the caller is responsible for saving the report).
    source_config / target_config : dict | None
        Pre-loaded config dicts.  When provided the corresponding
        ``*_config_path`` is not read, avoiding redundant I/O.
    source_tables / target_tables : list[str] | None
        Pre-discovered table lists.  When provided the table-discovery
        step is skipped entirely, avoiding redundant database connections.
    tables_file : str | None
        Override for the tables CSV path.  Only used when table lists are
        not supplied directly.
    source_connector / target_connector : DBConnector | None
        Already-connected database connectors.  When provided the
        metadata fetch reuses these connections instead of opening new
        ones.  The caller owns the connection lifecycle.
    """
    with Timer("Database column comparison"):
        if source_config is None:
            source_config = load_config(source_config_path)
        if target_config is None:
            target_config = load_config(target_config_path)

        # Merge excluded_cols from source/target connection configs
        # so users don't need a separate type-config file just for this.
        cfg_excluded = collect_excluded_cols(None, source_config, target_config)
        if cfg_excluded:
            if excluded_cols is None:
                excluded_cols = cfg_excluded
            else:
                excluded_cols = excluded_cols | cfg_excluded

        # Discover tables only when the caller hasn't provided them
        if source_tables is None:
            if tables_file is None:
                tables_file = source_config.get("tables_file") or target_config.get(
                    "tables_file"
                )
            _df, source_tables, target_tables = _read_table_lists(
                tables_file, source_config, target_config
            )
        else:
            if tables_file is None:
                tables_file = source_config.get("tables_file") or target_config.get(
                    "tables_file"
                )

        if target_tables is None:
            target_tables = source_tables

        # Fetch all column metadata per schema in a single query each,
        # then filter to only the tables we care about.
        source_df = fetch_all_metadata_as_df(
            source_config, source_tables, connector=source_connector
        )
        target_df = fetch_all_metadata_as_df(
            target_config, target_tables, connector=target_connector
        )

        if tables_file:
            report_name = Path(tables_file).stem
        else:
            from dbqt.tools.utils import get_schema_label

            schema_label = get_schema_label(target_config)
            report_name = f"{schema_label}_colcompare_report"

        # Build comparison results
        table_comparison = compare_tables(source_df, target_df)
        column_comparisons = []
        for tbl in table_comparison["common"]:
            cc = compare_columns(
                source_df, target_df, tbl, type_mappings, excluded_cols
            )
            column_comparisons.append({"table_name": tbl, **cc})
        comp_results = {"tables": table_comparison, "columns": column_comparisons}

        # If an external report was provided, append tabs to it
        # Skip "Table Comparison" tab — the row-counts tab already covers that.
        if report is not None:
            tabs = build_colcompare_tabs(
                comp_results, source_df, target_df, type_mappings, excluded_cols
            )
            for tab_name, columns, data in tabs:
                if tab_name == "Table Comparison":
                    continue
                report.add_tab(tab_name, columns=columns, data=data)
            logger.info("Database column comparison complete (tabs added to report)")
            return None

        # Standalone mode — write our own HTML file
        output_path = create_html_report(
            comp_results,
            source_df,
            target_df,
            report_name,
            type_mappings,
            excluded_cols,
        )
        logger.info("Database column comparison complete")
        return output_path


def colcompare(args=None):
    """Run column comparison from files or database connections."""
    if isinstance(args, (list, type(None))):
        parser = argparse.ArgumentParser(
            description="Compare column schemas between CSV/Parquet files or databases",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Generates an Excel report with three sheets:
- Table Comparison: Lists tables present in source/target
- Column Comparison: Details of column presence and type matching
- Datatype Mismatches: Highlights columns with incompatible types

The report is saved to ./results/ with a timestamp in the filename.

To generate a default configuration file:
  dbqt colcompare --generate-col-mappings [--output PATH]
            """,
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
            help="Output path for config file (used with --generate-config)",
        )
        parser.add_argument(
            "source", nargs="?", help="Path to the source CSV/Parquet file"
        )
        parser.add_argument(
            "target", nargs="?", help="Path to the target CSV/Parquet file"
        )
        parser.add_argument("--source-config", help="YAML config for source database")
        parser.add_argument("--target-config", help="YAML config for target database")
        parser.add_argument(
            "--config", "-c", help="Path to type mappings configuration file"
        )
        parser.add_argument(
            "--excluded-cols",
            nargs="*",
            default=None,
            help="Column names to exclude from comparison (overrides config file)",
        )
        parser.add_argument(
            "--verbose", "-v", action="store_true", help="Verbose logging"
        )

        args = parser.parse_args(args)
        setup_logging(args.verbose)

        if args.generate_col_mappings:
            generate_config_file(args.output)
            return

        # Build excluded_cols set: CLI flag merges with config file
        config_path = getattr(args, "config", None)
        if args.excluded_cols is not None:
            excluded_cols = {c.upper() for c in args.excluded_cols}
        else:
            excluded_cols = load_excluded_cols(config_path)

        if args.source_config and args.target_config:
            tm = load_type_mappings(config_path)
            excluded_cols = collect_excluded_cols(
                config_path,
                load_config(args.source_config),
                load_config(args.target_config),
            )
            colcompare_from_db(
                args.source_config, args.target_config, tm, excluded_cols
            )
            return

        if not args.source or not args.target:
            parser.error(
                "Provide source and target files, or --source-config and --target-config"
            )

    config_path = getattr(args, "config", None)
    type_mappings = load_type_mappings(config_path)
    if getattr(args, "excluded_cols", None) is not None:
        excluded_cols = {c.upper() for c in args.excluded_cols}
    else:
        excluded_cols = load_excluded_cols(config_path)

    with Timer("Column comparison"):
        source_df, target_df = read_files(args.source, args.target)
        target_file_name = args.target.split(".")[0]
        _run_comparison(
            source_df, target_df, target_file_name, type_mappings, excluded_cols
        )


def main(args=None):
    """Entry point for the colcompare tool."""
    colcompare(args)


if __name__ == "__main__":
    main()
