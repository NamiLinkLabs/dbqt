"""Shared utilities for dbqt tools."""

import re
import csv
import json
import yaml
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dbqt.connections import (  # noqa: F401 — re-export
    create_connector,
    normalize_table_path,
    build_qualified_table_name,
)

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


def get_schema_label(config):
    """Extract a human-readable schema/database label from a YAML config.

    Tries ``schema``, then ``database``, then ``sid``, falling back to
    ``"default"``.  Used for naming output files during auto-discovery.
    """
    conn = config.get("connection", config)
    for key in ("schema", "database", "sid"):
        val = conn.get(key)
        if val:
            return val
    return "default"


def filter_excluded_tables(tables, excluded_patterns):
    """Filter out tables matching any of the exclusion patterns.

    Patterns use SQL-like wildcards: ``%`` matches any sequence of
    characters.  Matching is case-insensitive.

    Example patterns: ``%_FINAL``, ``TMP_%``, ``%_BAK_%``
    """
    if not excluded_patterns:
        return tables

    compiled = []
    for pat in excluded_patterns:
        # Escape regex metacharacters, then convert SQL LIKE wildcards
        # % and _ are not regex special chars so re.escape leaves them alone
        regex = re.escape(pat.upper()).replace("%", ".*").replace("_", ".")
        compiled.append(re.compile(f"^{regex}$", re.IGNORECASE))

    def _excluded(t):
        leaf = t.rsplit(".", 1)[-1]
        return any(r.match(t) or r.match(leaf) for r in compiled)

    filtered = [t for t in tables if not _excluded(t)]
    removed = len(tables) - len(filtered)
    if removed:
        logger.info(
            f"Excluded {removed} table(s) matching patterns: {excluded_patterns}"
        )
    return filtered


def discover_tables_from_db(config, connector=None):
    """Connect to a database and list all tables in the configured schema.

    If *connector* is provided (already connected), it will be reused and
    the caller owns the connection lifecycle.  Otherwise a temporary
    connection is created and torn down automatically.

    Returns a list of table name strings.
    """
    owns_connection = connector is None
    if owns_connection:
        connector = create_connector(config["connection"])
        connector.connect()
    try:
        tables = connector.list_tables()
    finally:
        if owns_connection:
            connector.disconnect()
    return tables


def _read_table_lists(
    tables_file,
    source_config=None,
    target_config=None,
    source_connector=None,
    target_connector=None,
):
    """Read the tables CSV and return (df, source_tables, target_tables).

    If *tables_file* is ``None``, tables are auto-discovered from the database
    schema defined in the YAML config(s).

    *source_connector* / *target_connector* — optional already-connected
    connectors to reuse for table discovery, avoiding extra SSO prompts.

    Tables matching ``excluded_tables`` patterns in the YAML config(s) are
    removed automatically.

    Returns target_tables=None when operating in single-config mode.
    """
    import polars as pl

    # Collect exclusion patterns from config(s)
    _exc_patterns = set()
    for cfg in (source_config, target_config):
        if cfg:
            for pat in cfg.get("excluded_tables", []):
                _exc_patterns.add(pat)
    exc_patterns = sorted(_exc_patterns) if _exc_patterns else []

    if tables_file is not None:
        df = pl.read_csv(tables_file)
        if "source_table" in df.columns and "target_table" in df.columns:
            src = filter_excluded_tables(df["source_table"].to_list(), exc_patterns)
            tgt = filter_excluded_tables(df["target_table"].to_list(), exc_patterns)
            df = df.filter(pl.col("source_table").is_in(src))
            return df, src, tgt
        elif "table_name" in df.columns:
            tables = filter_excluded_tables(df["table_name"].to_list(), exc_patterns)
            df = df.filter(pl.col("table_name").is_in(tables))
            return df, tables, None
        else:
            raise ValueError(
                "CSV must contain 'table_name' or 'source_table'/'target_table' columns."
            )

    # Auto-discover tables from database(s)
    logger.info("No tables_file provided — discovering tables from database schema")

    if source_config and target_config and source_config is not target_config:
        source_tables_raw = filter_excluded_tables(
            discover_tables_from_db(source_config, connector=source_connector),
            exc_patterns,
        )
        target_tables_raw = filter_excluded_tables(
            discover_tables_from_db(target_config, connector=target_connector),
            exc_patterns,
        )

        # Build case-insensitive lookup maps: upper -> original
        source_map = {t.upper(): t for t in source_tables_raw}
        target_map = {t.upper(): t for t in target_tables_raw}

        source_upper = set(source_map.keys())
        target_upper = set(target_map.keys())

        common_upper = sorted(source_upper & target_upper)
        source_only_upper = sorted(source_upper - target_upper)
        target_only_upper = sorted(target_upper - source_upper)

        rows = []
        for u in common_upper:
            rows.append(
                {
                    "source_table": source_map[u],
                    "target_table": target_map[u],
                    "_discovery_status": "common",
                }
            )
        for u in source_only_upper:
            rows.append(
                {
                    "source_table": source_map[u],
                    "target_table": source_map[u],
                    "_discovery_status": "source_only",
                }
            )
        for u in target_only_upper:
            rows.append(
                {
                    "source_table": target_map[u],
                    "target_table": target_map[u],
                    "_discovery_status": "target_only",
                }
            )

        df = pl.DataFrame(rows)
        return (
            df,
            df["source_table"].to_list(),
            df["target_table"].to_list(),
        )
    elif source_config:
        tables = filter_excluded_tables(
            discover_tables_from_db(source_config, connector=source_connector),
            exc_patterns,
        )
        df = pl.DataFrame({"table_name": tables})
        return df, tables, None
    else:
        raise ValueError(
            "Cannot discover tables: no config provided and no tables_file specified"
        )


def get_metadata_for_table(connector, table_name, prefix=""):
    """Fetch column metadata for a single table via fetch_table_metadata."""
    import threading

    threading.current_thread().name = f"Meta-{prefix}{table_name}"
    try:
        metadata = connector.fetch_table_metadata(table_name)
        logger.info(f"Table {prefix}{table_name}: {len(metadata)} columns")
        return table_name, (metadata, None)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error getting metadata for {prefix}{table_name}: {error_msg}")
        return table_name, (None, error_msg)


def metadata_to_df(results, table_names):
    """Convert metadata results dict into a Polars DataFrame."""
    import polars as pl

    rows = []
    for table_name in table_names:
        metadata, error = results[table_name]
        if metadata is None:
            continue
        for col_row in metadata:
            rows.append(
                {
                    "SCH_TABLE": table_name,
                    "COL_NAME": str(col_row[0]).upper(),
                    "DATA_TYPE": str(col_row[1]).upper() if col_row[1] else "N/A",
                    "DATETIME_PRECISION": (
                        col_row[2]
                        if len(col_row) > 2 and col_row[2] is not None
                        else None
                    ),
                    "NUMERIC_PRECISION": (
                        col_row[3]
                        if len(col_row) > 3 and col_row[3] is not None
                        else None
                    ),
                    "NUMERIC_SCALE": (
                        col_row[4]
                        if len(col_row) > 4 and col_row[4] is not None
                        else None
                    ),
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "SCH_TABLE": pl.Utf8,
                "COL_NAME": pl.Utf8,
                "DATA_TYPE": pl.Utf8,
                "DATETIME_PRECISION": pl.Int64,
                "NUMERIC_PRECISION": pl.Int64,
                "NUMERIC_SCALE": pl.Int64,
            }
        )
    return pl.DataFrame(rows)


def fetch_all_metadata_as_df(config, table_names=None, connector=None):
    """Fetch column metadata for an entire schema in ONE query, return a Polars DataFrame.

    If *table_names* is provided the result is filtered to only those tables.
    This is much faster than ``fetch_metadata_parallel`` when many tables are
    involved because it issues a single ``SELECT … FROM information_schema.columns``
    (or equivalent) instead of one query per table.

    Parameters
    ----------
    connector : DBConnector | None
        An already-connected database connector.  When provided the caller
        owns the connection lifecycle — this function will neither connect
        nor disconnect it.
    """
    import polars as pl

    owns_connection = connector is None
    if owns_connection:
        connector = create_connector(config["connection"])
        connector.connect()
    try:
        raw = connector.fetch_schema_metadata()
    finally:
        if owns_connection:
            connector.disconnect()

    # raw rows: (table_name, column_name, data_type, dt_prec, num_prec, num_scale)
    if not raw:
        return pl.DataFrame(
            schema={
                "SCH_TABLE": pl.Utf8,
                "COL_NAME": pl.Utf8,
                "DATA_TYPE": pl.Utf8,
                "DATETIME_PRECISION": pl.Int64,
                "NUMERIC_PRECISION": pl.Int64,
                "NUMERIC_SCALE": pl.Int64,
            }
        )

    rows = [
        {
            "SCH_TABLE": str(r[0]).upper(),
            "COL_NAME": str(r[1]).upper(),
            "DATA_TYPE": str(r[2]).upper() if r[2] else "N/A",
            "DATETIME_PRECISION": r[3],
            "NUMERIC_PRECISION": r[4],
            "NUMERIC_SCALE": r[5],
        }
        for r in raw
    ]
    df = pl.DataFrame(rows)

    if table_names is not None:
        upper_names = {t.upper() for t in table_names}
        df = df.filter(pl.col("SCH_TABLE").is_in(upper_names))

    return df


# ---------------------------------------------------------------------------
# HTML report builder (Tabulator-based)
# ---------------------------------------------------------------------------

_TABULATOR_CSS = (
    "https://unpkg.com/tabulator-tables@6.3.1/dist/css/tabulator_midnight.min.css"
)
_TABULATOR_JS = "https://unpkg.com/tabulator-tables@6.3.1/dist/js/tabulator.min.js"
_XLSX_JS = "https://cdn.sheetjs.com/xlsx-0.20.3/package/dist/xlsx.full.min.js"


class HTMLReport:
    """Build a self-contained HTML file with tabbed Tabulator tables.

    Usage::

        report = HTMLReport("My Report")
        report.add_tab("Row Counts", columns=[...], data=[...])
        report.add_tab("Columns", columns=[...], data=[...])
        report.save("results/report.html")
    """

    def __init__(self, title: str = "Report"):
        self.title = title
        self.tabs: list[dict] = []

    # -- helpers to build Tabulator column definitions ----------------------

    @staticmethod
    def columns_from_names(names: list[str]) -> list[dict]:
        """Create simple Tabulator column definitions from a list of names."""
        return [
            {"title": n, "field": n, "headerFilter": "input", "sorter": "string"}
            for n in names
        ]

    @staticmethod
    def columns_from_polars(df) -> list[dict]:
        """Infer Tabulator column definitions from a Polars DataFrame."""
        import polars as pl

        cols = []
        for name in df.columns:
            dtype = df[name].dtype
            if dtype in (
                pl.Int8,
                pl.Int16,
                pl.Int32,
                pl.Int64,
                pl.UInt8,
                pl.UInt16,
                pl.UInt32,
                pl.UInt64,
                pl.Float32,
                pl.Float64,
            ):
                sorter = "number"
                hf = "number"
                formatter = "plaintext"
            else:
                sorter = "string"
                hf = "input"
                formatter = "plaintext"
            cols.append(
                {
                    "title": name,
                    "field": name,
                    "headerFilter": hf,
                    "sorter": sorter,
                    "formatter": formatter,
                }
            )
        return cols

    # -- adding tabs --------------------------------------------------------

    def add_tab(self, name: str, *, columns: list[dict], data: list[dict]):
        """Append a tab to the report.

        *columns* – list of Tabulator column definition dicts.
        *data*    – list of row dicts (JSON-serialisable).
        """
        self.tabs.append({"name": name, "columns": columns, "data": data})

    def add_polars_tab(self, name: str, df):
        """Convenience: add a tab directly from a Polars DataFrame."""
        columns = self.columns_from_polars(df)
        data = df.to_dicts()
        # Ensure all values are JSON-serialisable (None is fine, but NaN is not)
        for row in data:
            for k, v in row.items():
                if isinstance(v, float) and (v != v):  # NaN check
                    row[k] = None
        self.add_tab(name, columns=columns, data=data)

    # -- rendering ----------------------------------------------------------

    def _render(self) -> str:
        tab_buttons = []
        tab_divs = []
        tab_scripts = []

        for i, tab in enumerate(self.tabs):
            tab_id = f"tab_{i}"
            active = "active" if i == 0 else ""
            display = "block" if i == 0 else "none"

            tab_buttons.append(
                f'<button class="tab-btn {active}" onclick="switchTab(event, \'{tab_id}\')">'
                f'{tab["name"]}</button>'
            )
            tab_divs.append(
                f'<div id="{tab_id}" class="tab-content" style="display:{display}">'
                f'  <div id="{tab_id}_table"></div>'
                f"</div>"
            )
            tab_scripts.append(
                f'tabulators["{tab_id}"] = new Tabulator("#{tab_id}_table", {{\n'
                f'  data: {json.dumps(tab["data"])},\n'
                f'  columns: {json.dumps(tab["columns"])},\n'
                f'  layout: "fitDataTable",\n'
                f'  height: "calc(100vh - 120px)",\n'
                f"  pagination: false,\n"
                f"  movableColumns: true,\n"
                f"  clipboard: true,\n"
                f"  downloadConfig: {{ columnHeaders: true }},\n"
                f"}});"
            )

        buttons_html = "\n".join(tab_buttons)
        divs_html = "\n".join(tab_divs)
        scripts_js = "\n".join(tab_scripts)

        return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{self.title}</title>
<link rel="stylesheet" href="{_TABULATOR_CSS}">
<script src="{_TABULATOR_JS}"></script>
<script src="{_XLSX_JS}"></script>
<style>
  body {{ font-family: Arial, sans-serif; margin: 0; padding: 10px; background: #1a1a2e; color: #eee; }}
  h1 {{ margin: 0 0 10px 0; font-size: 1.3em; }}
  .tab-bar {{ display: flex; gap: 4px; margin-bottom: 8px; flex-wrap: wrap; align-items: center; }}
  .tab-btn {{
    padding: 6px 16px; border: none; cursor: pointer; border-radius: 4px 4px 0 0;
    background: #16213e; color: #aaa; font-size: 0.9em;
  }}
  .tab-btn.active {{ background: #0f3460; color: #fff; }}
  .tab-btn:hover {{ background: #0f3460; color: #fff; }}
  .export-btn {{
    margin-left: auto; padding: 6px 14px; border: 1px solid #555; border-radius: 4px;
    background: #16213e; color: #ccc; cursor: pointer; font-size: 0.85em;
  }}
  .export-btn:hover {{ background: #0f3460; color: #fff; }}
  .tab-content {{ display: none; }}
</style>
</head>
<body>
<h1>{self.title}</h1>
<div class="tab-bar">
  {buttons_html}
  <button class="export-btn" onclick="exportXLSX()">⬇ Export XLSX</button>
</div>
{divs_html}
<script>
var tabulators = {{}};
function switchTab(evt, tabId) {{
  document.querySelectorAll('.tab-content').forEach(d => d.style.display = 'none');
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(tabId).style.display = 'block';
  evt.currentTarget.classList.add('active');
}}
function exportXLSX() {{
  /* Build a multi-sheet XLSX using SheetJS from all Tabulator instances */
  var wb = XLSX.utils.book_new();
  var tabs = {json.dumps([t["name"] for t in self.tabs])};
  for (var i = 0; i < tabs.length; i++) {{
    var tabId = "tab_" + i;
    var instance = tabulators[tabId];
    var data = instance ? instance.getData() : [];
    var ws = XLSX.utils.json_to_sheet(data);
    XLSX.utils.book_append_sheet(wb, ws, tabs[i].substring(0, 31));
  }}
  XLSX.writeFile(wb, "{self.title}.xlsx");
}}
{scripts_js}
</script>
</body>
</html>"""

    def save(self, path: str) -> str:
        """Write the HTML report to *path* and return the path."""
        import os

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(self._render())
        logger.info(f"Report saved to {path}")
        return path


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
