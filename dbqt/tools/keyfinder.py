import argparse
import logging
import polars as pl
from itertools import combinations
from typing import List, Tuple, Optional, Set, Dict, Any
from collections import defaultdict
from dbqt.tools.utils import load_config, setup_logging, Timer
from dbqt.connections import create_connector
from math import comb
from tqdm import tqdm

logger = logging.getLogger(__name__)


# ============================================================================
# GORDIAN Algorithm Implementation
# ============================================================================


class PrefixTreeCell:
    """A cell in a prefix tree node containing a value and metadata."""

    def __init__(self, value, is_leaf=False):
        self.value = value
        self.child = None  # Points to child node (for non-leaf cells)
        self.count = 0  # Counter for leaf cells
        self.sum_counts = 0  # Sum of all descendant leaf counts
        self.is_leaf = is_leaf


class PrefixTreeNode:
    """A node in the prefix tree."""

    def __init__(self, attr_no):
        self.attr_no = attr_no  # Attribute number (level in tree)
        self.cells = {}  # Dict mapping value -> PrefixTreeCell
        self.is_leaf = False
        self.visited = False  # For singleton pruning

    def get_or_create_cell(self, value, is_leaf=False):
        """Get existing cell or create new one."""
        if value not in self.cells:
            self.cells[value] = PrefixTreeCell(value, is_leaf)
        return self.cells[value]

    def has_multiple_cells(self):
        """Check if node has more than one cell."""
        return len(self.cells) > 1

    def get_total_count(self):
        """Get sum of all counts in this node."""
        return sum(cell.sum_counts for cell in self.cells.values())


class NonKeySet:
    """Container for maintaining non-redundant non-keys using bitmaps."""

    def __init__(self, num_attributes):
        self.num_attributes = num_attributes
        self.non_keys = []  # List of bitmaps representing non-keys

    def _to_bitmap(self, attr_list):
        """Convert list of attribute indices to bitmap."""
        bitmap = 0
        for attr in attr_list:
            bitmap |= 1 << attr
        return bitmap

    def _from_bitmap(self, bitmap):
        """Convert bitmap to list of attribute indices."""
        attrs = []
        for i in range(self.num_attributes):
            if bitmap & (1 << i):
                attrs.append(i)
        return attrs

    def _covers(self, nk1_bitmap, nk2_bitmap):
        """Check if nk1 covers nk2 (nk2 is subset of nk1)."""
        return (nk2_bitmap & nk1_bitmap) == nk2_bitmap

    def insert(self, attr_list):
        """Insert a non-key, maintaining non-redundancy."""
        new_bitmap = self._to_bitmap(attr_list)

        # Check if any existing non-key covers the new one
        for existing_bitmap in self.non_keys:
            if self._covers(existing_bitmap, new_bitmap):
                return False  # Redundant, don't add

        # Remove any existing non-keys that are now covered by new one
        self.non_keys = [bm for bm in self.non_keys if not self._covers(new_bitmap, bm)]

        # Add the new non-key
        self.non_keys.append(new_bitmap)
        return True

    def is_futile(self, attr_list):
        """Check if searching this attribute combination is futile."""
        candidate_bitmap = self._to_bitmap(attr_list)

        # Check if any existing non-key is a subset of candidate
        for nk_bitmap in self.non_keys:
            if self._covers(candidate_bitmap, nk_bitmap):
                return True
        return False

    def get_non_keys(self):
        """Return list of non-keys as attribute lists."""
        return [self._from_bitmap(bm) for bm in self.non_keys]


def build_prefix_tree(
    connector, table_name: str, columns: List[str], verbose: bool = False
) -> Optional[PrefixTreeNode]:
    """
    Build a prefix tree from the table data.

    Returns None if table has no keys (duplicate entities found).
    """
    logger.info(f"Building prefix tree for {table_name}...")

    # Create root node
    root = PrefixTreeNode(0)

    # Fetch all data
    try:
        query = f"SELECT {', '.join(columns)} FROM {table_name}"
        result = connector.run_query(query)

        if not result:
            logger.warning(f"No data in table {table_name}")
            return root

        total_rows = len(result)
        logger.info(f"Processing {total_rows:,} rows...")

        # Process each row with progress bar
        pbar = tqdm(
            result,
            total=total_rows,
            desc="Building prefix tree",
            unit="rows",
            disable=not verbose,
        )

        for row_idx, row in enumerate(pbar):
            node = root

            # Process each attribute value in the row
            for attr_idx, value in enumerate(row):
                is_last = attr_idx == len(row) - 1

                # Get or create cell for this value
                cell = node.get_or_create_cell(value, is_leaf=is_last)

                if is_last:
                    # Leaf cell - increment count
                    cell.count += 1
                    if cell.count > 1:
                        # Found duplicate entity - no keys exist
                        logger.error(f"Duplicate entity found at row {row_idx + 1}")
                        pbar.close()
                        return None
                    cell.sum_counts = cell.count
                else:
                    # Non-leaf cell - create child if needed
                    if cell.child is None:
                        cell.child = PrefixTreeNode(attr_idx + 1)
                        cell.child.is_leaf = is_last
                    node = cell.child

        pbar.close()

        # Update sum_counts bottom-up
        _update_sum_counts(root)

        logger.info(f"Prefix tree built successfully")
        return root

    except Exception as e:
        logger.error(f"Error building prefix tree: {e}")
        raise


def _update_sum_counts(node: PrefixTreeNode):
    """Recursively update sum_counts for all nodes."""
    for cell in node.cells.values():
        if cell.is_leaf:
            cell.sum_counts = cell.count
        else:
            if cell.child:
                _update_sum_counts(cell.child)
                cell.sum_counts = cell.child.get_total_count()


def merge_nodes(nodes: List[PrefixTreeNode], attr_no: int) -> PrefixTreeNode:
    """
    Merge multiple prefix tree nodes into a single node.

    This implements the cube computation by merging projections.
    """
    if len(nodes) == 1:
        # Single node - just return it (shared reference)
        return nodes[0]

    # Create merged node
    merged = PrefixTreeNode(attr_no)
    merged.is_leaf = nodes[0].is_leaf

    # Collect all distinct values across nodes
    all_values = set()
    for node in nodes:
        all_values.update(node.cells.keys())

    # For each distinct value, merge the cells
    for value in all_values:
        merged_cell = merged.get_or_create_cell(value, merged.is_leaf)

        if merged.is_leaf:
            # Leaf node - sum the counts
            for node in nodes:
                if value in node.cells:
                    merged_cell.count += node.cells[value].count
            merged_cell.sum_counts = merged_cell.count
        else:
            # Non-leaf node - recursively merge children
            child_nodes = []
            for node in nodes:
                if value in node.cells and node.cells[value].child:
                    child_nodes.append(node.cells[value].child)

            if child_nodes:
                merged_cell.child = merge_nodes(child_nodes, attr_no + 1)
                merged_cell.sum_counts = merged_cell.child.get_total_count()

    return merged


def find_non_keys_gordian(
    root: PrefixTreeNode, columns: List[str], verbose: bool = False
) -> NonKeySet:
    """
    Find all non-keys using the GORDIAN algorithm.

    This implements the interleaved cube computation with pruning.
    """
    logger.info("Finding non-keys using GORDIAN algorithm...")

    non_key_set = NonKeySet(len(columns))
    cur_non_key = []  # Current attribute path being explored

    # Progress tracking
    nodes_visited = [0]
    pbar = tqdm(desc="Finding non-keys", unit=" nodes", disable=not verbose)

    def non_key_finder(node: PrefixTreeNode):
        """Recursive function to find non-keys (Algorithm 4)."""
        nonlocal cur_non_key

        # Update progress
        nodes_visited[0] += 1
        pbar.update(1)
        pbar.set_postfix({"non-keys": len(non_key_set.non_keys)})

        # Add current attribute to path
        cur_non_key.append(node.attr_no)

        if node.is_leaf:
            # At leaf level - check for non-keys
            # Check if any cell has count > 1
            for cell in node.cells.values():
                if cell.count > 1:
                    non_key_set.insert(cur_non_key[:])
                    break

            # Remove current attribute
            cur_non_key.pop()

            # Check if projection without last attribute is a non-key
            if node.has_multiple_cells() or any(
                c.count > 1 for c in node.cells.values()
            ):
                if cur_non_key:  # Don't insert empty non-key
                    non_key_set.insert(cur_non_key[:])
        else:
            # Non-leaf node

            # Singleton pruning: skip if only one entity
            if node.get_total_count() == 1:
                cur_non_key.pop()
                return

            # Traverse all children (depth-first)
            for cell in node.cells.values():
                if cell.child and not cell.child.visited:
                    # Mark as visited for singleton pruning
                    cell.child.visited = True
                    non_key_finder(cell.child)

            # Remove current attribute
            cur_non_key.pop()

            # Merge children and continue exploration
            if node.has_multiple_cells():
                # Futility pruning: check if this search is futile
                if non_key_set.is_futile(cur_non_key[:]):
                    if verbose:
                        logger.debug(f"Futility pruning at {cur_non_key}")
                    return

                # Merge all children
                child_nodes = [
                    cell.child for cell in node.cells.values() if cell.child is not None
                ]

                if child_nodes:
                    merged = merge_nodes(child_nodes, node.attr_no + 1)
                    non_key_finder(merged)

    # Start the recursive search
    non_key_finder(root)
    pbar.close()

    non_keys = non_key_set.get_non_keys()
    logger.info(
        f"Found {len(non_keys)} non-redundant non-keys (visited {nodes_visited[0]} nodes)"
    )

    if verbose:
        for nk in non_keys:
            nk_names = [columns[i] for i in nk]
            logger.info(f"  Non-key: {nk_names}")

    return non_key_set


def convert_non_keys_to_keys(
    non_key_set: NonKeySet, columns: List[str], verbose: bool = False
) -> List[Tuple[str, ...]]:
    """
    Convert non-keys to minimal keys (Algorithm 6).

    Computes the cartesian product of complement sets.
    """
    non_keys = non_key_set.get_non_keys()

    if not non_keys:
        # No non-keys means all attributes together form a key
        return [tuple(columns)]

    logger.info(f"Converting {len(non_keys)} non-keys to keys...")

    # Start with empty key set
    key_set = []

    # Process non-keys with progress bar
    for nk_indices in tqdm(
        non_keys, desc="Converting to keys", unit="non-keys", disable=not verbose
    ):
        # Compute complement: attributes NOT in this non-key
        complement = []
        for i, col in enumerate(columns):
            if i not in nk_indices:
                complement.append((i,))  # Single-attribute key candidate

        if not complement:
            # Non-key covers all attributes - no keys possible
            logger.warning("Non-key covers all attributes - no keys exist")
            return []

        if not key_set:
            # First non-key - initialize key set with complement
            key_set = complement
        else:
            # Compute cartesian product with existing keys
            new_key_set = []
            for partial_key in complement:
                for existing_key in key_set:
                    # Union of partial key and existing key
                    combined = tuple(sorted(set(existing_key) | set(partial_key)))
                    new_key_set.append(combined)

            # Remove redundant keys (supersets)
            key_set = _remove_redundant_keys(new_key_set)

    # Convert indices to column names
    result = []
    for key_indices in key_set:
        key_names = tuple(columns[i] for i in key_indices)
        result.append(key_names)

    logger.info(f"Found {len(result)} minimal key(s)")
    return result


def _remove_redundant_keys(keys: List[Tuple[int, ...]]) -> List[Tuple[int, ...]]:
    """Remove redundant keys (keys that are supersets of other keys)."""
    minimal_keys = []

    for key in keys:
        key_set = set(key)
        is_minimal = True

        # Check if this key is a superset of any existing minimal key
        for minimal_key in minimal_keys:
            if set(minimal_key).issubset(key_set) and set(minimal_key) != key_set:
                is_minimal = False
                break

        if is_minimal:
            # Remove any existing keys that are supersets of this key
            minimal_keys = [
                mk
                for mk in minimal_keys
                if not key_set.issubset(set(mk)) or key_set == set(mk)
            ]
            minimal_keys.append(key)

    return minimal_keys


def find_keys_gordian(
    connector, table_name: str, columns: List[str], verbose: bool = False
) -> List[Tuple[str, ...]]:
    """
    Find all minimal composite keys using the GORDIAN algorithm.

    This is the main entry point for the GORDIAN algorithm.
    """
    logger.info(f"Running GORDIAN algorithm on {table_name}")
    logger.info(f"Analyzing {len(columns)} columns")

    # Step 1: Build prefix tree
    prefix_tree = build_prefix_tree(connector, table_name, columns, verbose)

    if prefix_tree is None:
        logger.warning(
            "Duplicate entities found, creating deduplicated table for GORDIAN analysis"
        )

        # Create a temporary deduplicated table
        dedup_table_name = f"temp_dedup_{table_name.replace('.', '_')}"
        try:
            col_list = ", ".join(columns)
            dedup_query = f"""
                CREATE OR REPLACE TABLE {dedup_table_name} AS
                SELECT {col_list}
                FROM {table_name}
                GROUP BY {col_list}
            """
            connector.run_query(dedup_query)
            logger.info(f"Created temporary deduplicated table: {dedup_table_name}")

            # Try building prefix tree again on deduplicated data
            prefix_tree = build_prefix_tree(
                connector, dedup_table_name, columns, verbose
            )

            # Clean up temporary table
            try:
                connector.run_query(f"DROP TABLE IF EXISTS {dedup_table_name}")
                logger.debug(f"Dropped temporary table: {dedup_table_name}")
            except Exception as cleanup_error:
                logger.warning(
                    f"Failed to drop temporary table {dedup_table_name}: {cleanup_error}"
                )

            if prefix_tree is None:
                logger.error("No keys exist even after deduplication")
                return []

        except Exception as e:
            logger.error(f"Failed to create deduplicated table: {e}")
            return []

    # Step 2: Find non-keys using interleaved cube computation
    non_key_set = find_non_keys_gordian(prefix_tree, columns, verbose)

    # Step 3: Convert non-keys to keys
    keys = convert_non_keys_to_keys(non_key_set, columns, verbose)

    return keys


# ============================================================================
# Original brute-force implementation (kept for compatibility)
# ============================================================================


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


def get_attribute_cardinalities(
    connector, table_name: str, columns: List[str]
) -> Dict[str, int]:
    """Get cardinality (distinct count) for each attribute, sorted descending"""
    cardinalities = {}

    for col in columns:
        try:
            query = f"SELECT COUNT(DISTINCT {col}) as card FROM {table_name}"
            result = connector.run_query(query)
            cardinalities[col] = result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get cardinality for {col}: {e}")
            cardinalities[col] = 0

    return cardinalities


def check_for_nulls(connector, table_name: str, columns: List[str]) -> Set[str]:
    """Check which columns contain NULL values"""
    columns_with_nulls = set()

    for col in columns:
        try:
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
            result = connector.run_query(query)
            null_count = result[0][0] if result else 0
            if null_count > 0:
                columns_with_nulls.add(col)
                logger.debug(f"Column {col} contains {null_count} NULL values")
        except Exception as e:
            logger.warning(f"Failed to check NULLs for {col}: {e}")

    return columns_with_nulls


def check_key_candidate(
    connector, table_name: str, columns: Tuple[str, ...], total_rows: int
) -> bool:
    """Check if a combination of columns forms a valid key (all values are unique)"""
    col_list = ", ".join(columns)
    query = f"""
        SELECT COUNT(*) as distinct_count
        FROM (
            SELECT {col_list}
            FROM {table_name}
            GROUP BY {col_list}
        ) subquery
    """

    try:
        result = connector.run_query(query)
        distinct_count = result[0][0] if result else 0
        return distinct_count == total_rows
    except Exception as e:
        logger.error(f"Error checking key candidate {columns}: {e}")
        return False


def find_minimal_keys(
    connector,
    table_name: str,
    columns: List[str],
    total_rows: int,
    max_key_size: int = None,
    verbose: bool = False,
) -> List[Tuple[str, ...]]:
    """
    Find all minimal keys by checking combinations in order of size.
    A minimal key is a key where no proper subset is also a key.
    """
    if max_key_size is None:
        max_key_size = len(columns)

    # Sort columns by cardinality (descending) for better performance
    cardinalities = get_attribute_cardinalities(connector, table_name, columns)
    sorted_columns = sorted(columns, key=lambda x: cardinalities[x], reverse=True)

    logger.info(
        f"Attribute cardinalities: {dict(sorted((k, v) for k, v in cardinalities.items()))}"
    )

    # Check for NULL values - any attribute with NULLs cannot be part of a key
    attrs_with_nulls = check_for_nulls(connector, table_name, sorted_columns)
    if attrs_with_nulls:
        logger.info(f"Attributes with NULLs (excluded from keys): {attrs_with_nulls}")
        sorted_columns = [a for a in sorted_columns if a not in attrs_with_nulls]
        if not sorted_columns:
            logger.warning("All attributes contain NULLs, no keys possible")
            return []

    minimal_keys = []
    checked_combinations = 0

    # Check combinations of increasing size
    for size in range(1, min(max_key_size, len(sorted_columns)) + 1):
        if verbose:
            logger.info(f"Checking attribute combinations of size {size}...")

        size_keys = []

        for combo in combinations(sorted_columns, size):
            # Skip if this is a superset of an already found minimal key
            is_superset_of_key = any(
                set(key).issubset(set(combo)) for key in minimal_keys
            )
            if is_superset_of_key:
                continue

            checked_combinations += 1

            # Check if this combination is a key
            if check_key_candidate(connector, table_name, combo, total_rows):
                logger.info(f"Key found: {combo}")
                size_keys.append(combo)

        # Add all keys found at this size level
        minimal_keys.extend(size_keys)

        # If we found keys at this size, don't check larger sizes
        # (they wouldn't be minimal)
        if minimal_keys:
            break

    logger.info(f"Checked {checked_combinations} combinations")
    return minimal_keys


def calculate_total_combinations(n_columns: int, max_size: int = None) -> int:
    """Calculate total number of combinations"""
    if max_size is None or max_size >= n_columns:
        return 2**n_columns - 1

    return sum(comb(n_columns, k) for k in range(1, max_size + 1))


def is_id_column(column_name: str) -> bool:
    """Check if column name looks like an ID column"""
    lower_name = column_name.lower()
    return (
        lower_name == "id"
        or "_id_" in lower_name
        or lower_name.startswith("id")
        or lower_name.endswith("id")
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
    use_gordian: bool = True,
) -> List[Tuple[str, ...]]:
    """
    Find all minimal composite keys.

    Uses GORDIAN algorithm by default for better performance on large datasets.
    Falls back to brute-force for small datasets or when GORDIAN is disabled.

    This checks combinations of columns in order of size, stopping when
    minimal keys are found. A minimal key is one where no proper subset
    is also a key.
    """
    total_rows = get_row_count(connector, table_name)

    if not total_rows or total_rows == 0:
        logger.warning(f"Table {table_name} is empty")
        return []

    logger.info(f"Total rows: {total_rows:,}")
    logger.info(f"Columns to analyze: {len(columns)}")

    if max_key_size is None:
        max_key_size = len(columns)

    # Limit columns for analysis if needed
    analysis_columns = (
        columns[:max_key_size] if len(columns) > max_key_size else columns
    )

    # Decide which algorithm to use
    total_combinations = calculate_total_combinations(
        len(analysis_columns), max_key_size
    )

    # Use GORDIAN for larger search spaces (>1000 combinations)
    if use_gordian and total_combinations > 1000:
        logger.info(f"Using GORDIAN algorithm ({total_combinations:,} combinations)")
        try:
            minimal_keys = find_keys_gordian(
                connector, table_name, analysis_columns, verbose
            )
        except Exception as e:
            logger.warning(
                f"GORDIAN algorithm failed: {e}, falling back to brute-force"
            )
            minimal_keys = find_minimal_keys(
                connector,
                table_name,
                analysis_columns,
                total_rows,
                max_key_size,
                verbose,
            )
    else:
        logger.info(
            f"Using brute-force algorithm ({total_combinations:,} combinations)"
        )
        minimal_keys = find_minimal_keys(
            connector, table_name, analysis_columns, total_rows, max_key_size, verbose
        )

    if minimal_keys:
        logger.info(f"Found {len(minimal_keys)} minimal key(s)")
        for key in minimal_keys:
            logger.info(f"  Key: {', '.join(key)}")
    else:
        logger.warning("No minimal keys found")

    return minimal_keys


def find_keys_for_table(
    connector,
    table_name: str,
    max_key_size: int = None,
    max_columns: int = None,
    exclude_columns: List[str] = None,
    include_columns: List[str] = None,
    force: bool = False,
    verbose: bool = False,
    sample_size: int = None,
    use_gordian: bool = True,
) -> Tuple[str, Tuple[Optional[str], Optional[str], Optional[str], bool]]:
    """Find composite keys for a single table and return formatted result

    Returns:
        Tuple of (original_table_name, (primary_key, error, dedup_table, dedup_needed))
        where dedup_needed is True if deduplication was required
    """
    # Track the original table name to return in results
    original_table_name = table_name
    # Track if we created a sample table that needs cleanup
    sample_table_name = None

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
                    logger.warning(f"Sample table is empty for {original_table_name}")
                    # Clean up empty sample table
                    connector.run_query(f"DROP TABLE IF EXISTS {sample_table_name}")
                    return original_table_name, (
                        None,
                        "Source table is empty",
                        None,
                        False,
                    )

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

        # Get ALL columns from the table (for deduplication)
        all_columns = get_column_names(connector, table_name)

        if not all_columns:
            logger.error(f"No columns found for table {original_table_name}")
            return original_table_name, (None, f"No columns found", None, False)

        # Start with all columns for key finding
        columns = all_columns.copy()

        # Filter columns for key finding
        if include_columns:
            columns = [col for col in columns if col in include_columns]

        if exclude_columns:
            columns = [col for col in columns if col not in exclude_columns]

        if not columns:
            logger.error(
                f"No columns remaining after filters for {original_table_name}"
            )
            return original_table_name, (None, "No columns after filters", None, False)
        # Prioritize ID columns
        columns = prioritize_id_columns(columns)
        # Check if number of columns exceeds limit for key finding
        if len(columns) > max_columns:
            logger.warning(
                f"Table {table_name} has {len(columns)} columns, limiting key search to first {max_columns}"
            )
            columns = columns[:max_columns]

        # Calculate combinations
        total_combinations = calculate_total_combinations(len(columns), max_key_size)

        logger.info(
            f"Table {original_table_name}: {len(columns)} columns, {total_combinations:,} combinations"
        )

        # Warn if too many combinations (only for brute-force algorithm)
        # GORDIAN can handle large search spaces efficiently
        if total_combinations > 50000 and not force and not use_gordian:
            error_msg = f"Too many combinations ({total_combinations:,}), use --force or enable GORDIAN algorithm"
            logger.error(f"{original_table_name}: {error_msg}")
            return original_table_name, (None, error_msg, None, False)

        # Find keys
        keys = find_composite_keys(
            connector, table_name, columns, max_key_size, verbose, use_gordian
        )

        # Check if table was empty (find_composite_keys returns empty list)
        total_rows = get_row_count(connector, table_name)
        if total_rows == 0:
            logger.warning(f"Table {original_table_name} is empty")
            return original_table_name, (None, "Table is empty", None, False)

        if keys:
            # Use the first (minimal) key found
            primary_key = keys[0]
            # Format as comma-separated list
            formatted_key = ", ".join(primary_key)
            logger.info(f"Found key for {original_table_name}: {formatted_key}")
            return original_table_name, (formatted_key, None, None, False)
        else:
            logger.warning(f"No composite keys found for {original_table_name}")

            # Try with deduplicated table
            logger.info(
                f"Attempting to find key on deduplicated version of {original_table_name}"
            )
            # Sanitize table name for dedup table (handle None and special characters)
            dedup_table_name = f"dedup_{original_table_name.replace('.', '_')}"
            try:
                # Create deduplicated table using ALL columns, not just the limited set for key finding
                all_col_list = ", ".join(all_columns)
                dedup_query = f"""
                    CREATE OR REPLACE TABLE {dedup_table_name} AS
                    SELECT {all_col_list}
                    FROM {table_name}
                    GROUP BY {all_col_list}
                """

                logger.info(f"Creating deduplicated table: {dedup_table_name}")
                connector.run_query(dedup_query)

                # Get row count for the dedup table
                try:
                    dedup_row_count = connector.count_rows(dedup_table_name)
                    logger.info(f"Deduplicated table has {dedup_row_count:,} rows")

                    if dedup_row_count == 0:
                        logger.warning(
                            f"Deduplicated table is empty for {original_table_name}"
                        )
                        # Drop the empty dedup table
                        try:
                            connector.run_query(
                                f"DROP TABLE IF EXISTS {dedup_table_name}"
                            )
                        except Exception as cleanup_error:
                            logger.warning(
                                f"Failed to drop empty dedup table {dedup_table_name}: {cleanup_error}"
                            )
                        return original_table_name, (
                            None,
                            "No keys found (deduplicated table is empty)",
                            None,
                            False,
                        )

                    # Now check for keys on deduplicated table
                    dedup_keys = find_composite_keys(
                        connector,
                        dedup_table_name,
                        columns,
                        max_key_size,
                        verbose,
                        use_gordian,
                    )

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
                    return original_table_name, (
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
                    logger.warning(f"{original_table_name}: {note}")
                    # Return the dedup table name and flag that dedup was needed
                    return original_table_name, (
                        formatted_key,
                        note,
                        dedup_table_name,
                        True,
                    )
                else:
                    # No key found even after dedup - clean up the dedup table
                    try:
                        connector.run_query(f"DROP TABLE IF EXISTS {dedup_table_name}")
                    except Exception as cleanup_error:
                        logger.warning(
                            f"Failed to drop dedup table {dedup_table_name}: {cleanup_error}"
                        )

                    logger.warning(
                        f"No keys found even after deduplication for {original_table_name}"
                    )
                    return original_table_name, (
                        None,
                        "No keys found (even after deduplication)",
                        None,
                        False,
                    )

            except Exception as dedup_error:
                logger.error(
                    f"Error during deduplication attempt for {original_table_name}: {dedup_error}"
                )
                return original_table_name, (
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
    max_columns: int = None,
    exclude_columns: List[str] = None,
    include_columns: List[str] = None,
    force: bool = False,
    verbose: bool = False,
    sample_size: int = None,
    use_gordian: bool = True,
):
    """
    Find composite keys in database table(s).

    Checks combinations of columns in order of size to find minimal keys.
    Uses cardinality-based ordering and NULL checking for better performance.
    """

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
                    use_gordian,
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
                    "ERROR: No table specified. You must provide either:\n"
                    "  1. --table <table_name> for a single table, OR\n"
                    "  2. --tables-file <csv_file> for multiple tables, OR\n"
                    "  3. 'tables_file' in your config YAML"
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
                        use_gordian,
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

                                    # Get ALL columns for target table (not limited by max_columns)
                                    try:
                                        target_all_columns = get_column_names(
                                            connector, target
                                        )

                                        if target_all_columns:
                                            # Create dedup table for target using ALL columns
                                            target_dedup_name = (
                                                f"dedup_{target.replace('.', '_')}"
                                            )
                                            all_col_list = ", ".join(target_all_columns)
                                            dedup_query = f"""
                                                CREATE OR REPLACE TABLE {target_dedup_name} AS
                                                SELECT {all_col_list}
                                                FROM {target}
                                                GROUP BY {all_col_list}
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
Finds minimal composite keys by checking combinations of columns in order of size.
Uses cardinality-based ordering and NULL checking for better performance.

Examples:
  %(prdog)s --config config.yaml --table users
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
        default=1000,
        help="Maximum number of columns to consider (default: 1000)",
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
    parser.add_argument(
        "--no-gordian",
        action="store_true",
        help="Disable GORDIAN algorithm and use brute-force approach",
    )

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    setup_logging(args.verbose)

    # Note: We don't validate here because tables_file can come from config YAML
    if args.table and args.tables_file:
        parser.error("Cannot specify both --table and --tables-file")

    # Check if at least one table source is provided (table or tables_file)
    # tables_file can also come from config, so we check that in keyfinder()
    if not args.table and not args.tables_file:
        # Load config to check if tables_file is there
        try:
            config = load_config(args.config)
            if not config.get("tables_file"):
                parser.error(
                    "You must specify either --table <table_name> or --tables-file <csv_file>\n"
                    "(or include 'tables_file' in your config YAML)"
                )
        except Exception as e:
            parser.error(f"Error loading config: {e}")

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
        use_gordian=not args.no_gordian,
    )


if __name__ == "__main__":
    main()
