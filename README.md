# DBQT (DataBase Quality Tool)

DBQT is a lightweight, Python-first data quality testing framework that helps data teams maintain high-quality data through automated checks and intelligent suggestions. 

## Current Tools

### Column Comparison Tool (dbqt colcompare / dbqt compare)
Compare schemas between databases or files:
- Table-level comparison
- Column-level comparison with data type compatibility checks
- Support for CSV and Parquet files
- Handles nested Parquet schemas (arrays, structs, maps)
- **Database-backed comparison** — fetch metadata directly from source/target databases
- Retrieves precision information (datetime, numeric precision, numeric scale)
- Supports `table`, `schema.table`, and `db.schema.table` patterns in CSV files
- Intelligent data type compatibility checking
- **Customizable type mappings via YAML configuration**
- Generates detailed Excel report with:
  - Table differences
  - Column differences
  - Data type mismatches
  - Formatted worksheets for easy analysis

Usage:
```bash
# Basic file-based comparison
dbqt colcompare source_schema.csv target_schema.csv

# Compare Parquet files directly
dbqt colcompare source.parquet target.parquet

# Database-backed comparison (fetches metadata from live databases)
dbqt colcompare --source-config source_config.yaml --target-config target_config.yaml

# Generate a default column type mappings configuration file (works with both colcompare and dbstats)
dbqt colcompare --generate-col-mappings

# Generate config with custom output path
dbqt colcompare --generate-col-mappings --output my_types.yaml

# Use custom type mappings for comparison
dbqt colcompare source.csv target.csv --config my_types.yaml

# Exclude specific columns from comparison (CLI flag)
dbqt colcompare source.csv target.csv --excluded-cols CREATED_AT UPDATED_AT

# Also available via alias
dbqt compare source.csv target.csv
```

**Customizing Type Mappings:**

The tool uses intelligent type compatibility checking (e.g., `INT` and `BIGINT` are considered compatible). You can customize these mappings:

1. Generate a default configuration file:
   ```bash
   dbqt compare --generate-col-mappings
   ```

2. Edit the generated `colcompare_config.yaml` file to add or modify type groups and excluded columns:
   ```yaml
   description: Column comparison type mappings configuration. Each key represents a type group, and the list contains equivalent types. excluded_cols is a list of column names to ignore during comparison.
   type_mappings:
     INTEGER:
     - INT
     - INTEGER
     - BIGINT
     - SMALLINT
     - TINYINT
     - NUMBER
     VARCHAR:
     - VARCHAR
     - TEXT
     - CHAR
     - STRING
     - NVARCHAR
     - VARCHAR2
     # Add your custom types here
   # Column names to exclude from comparison (case-insensitive)
   excluded_cols:
     - CREATED_AT
     - UPDATED_AT
   ```

3. Use your custom configuration:
   ```bash
   dbqt compare source.csv target.csv --config colcompare_config.yaml
   ```

To generate CSV schema files from your database, run this query:
```sql
SELECT
    upper(table_schema) as SCH, --optional
    upper(table_name) as TABLE_NAME,
    upper(column_name) as COL_NAME,
    upper(data_type) as DATA_TYPE --optional
FROM information_schema.columns
where UPPER(table_schema) = UPPER('YOUR_SCHEMA')
order by table_name, ordinal_position;
```

Export the results to CSV format to use with the compare tool.

### Parquet Combine Tool (dbqt combine)
Combine multiple Parquet files into a single file:
- Validates schema compatibility
- Preserves nested data structures
- Handles large datasets efficiently

Usage:
```bash
dbqt combine [output.parquet]  # Combines all .parquet files in current directory
```

### Database Statistics Tool (dbqt dbstats / dbqt rowcount / dbqt stats)
Collect and analyze database statistics with multiple modes:
- **rowcount** (default) — Fetches table row counts in parallel
- **colcompare** — Compares column schemas between source and target databases
- **both** — Runs row counts then column comparison in one command
- Supports both single table analysis and source/target table comparisons
- Supports separate source and target database configurations for cross-database comparisons
- Supports `table`, `schema.table`, and `db.schema.table` patterns in CSV files
- Automatically calculates differences and percentage changes for comparisons
- Updates statistics in a CSV file with comprehensive error reporting
- Configurable through YAML

Usage:
```bash
# Row counts — single database mode
dbqt dbstats --config config.yaml

# Row counts — dual database mode (source and target in different databases)
dbqt dbstats --source-config source_config.yaml --target-config target_config.yaml

# Column comparison via dbstats
dbqt dbstats colcompare --source-config source_config.yaml --target-config target_config.yaml

# Both row counts and column comparison in one run
dbqt dbstats both --source-config source_config.yaml --target-config target_config.yaml

# Generate a default column type mappings configuration file
dbqt dbstats --generate-col-mappings

# Also available via aliases
dbqt rowcount --config config.yaml
dbqt stats --config config.yaml
```

Example config.yaml (single database mode):
```yaml
# Database connection configuration
connection:
  type: mysql  # mysql, snowflake, duckdb, csv, parquet, s3parquet, postgresql, sqlserver, athena
  host: localhost
  user: myuser
  password: mypassword
  database: mydb
  # Optional AWS configs for s3parquet
  # aws_profile: default
  # aws_region: us-west-2
  # bucket: my-bucket

  # Snowflake-specific configs
  # type: snowflake
  # account: your_account.region
  # warehouse: YOUR_WAREHOUSE
  # database: YOUR_DB
  # schema: YOUR_SCHEMA
  # role: YOUR_ROLE
  # authenticator: externalbrowser  # Optional: use SSO authentication
  # user: your_username
  # password: your_password  # Not needed if using externalbrowser auth

# Path to CSV file containing table names to analyze
tables_file: tables.csv

# Optional: number of parallel workers (default: 4)
max_workers: 10
```

Example dual database configuration (for cross-database comparisons):

source_config.yaml:
```yaml
connection:
  type: mysql
  host: source-db.example.com
  user: source_user
  password: source_pass
  database: source_db

tables_file: tables.csv  # CSV with source_table and target_table columns
max_workers: 10
```

target_config.yaml:
```yaml
connection:
  type: snowflake
  account: myorg.snowflakecomputing.com
  user: target_user
  password: target_pass
  warehouse: COMPUTE_WH
  database: TARGET_DB
  schema: PUBLIC
  role: ANALYST
```

The tables.csv file should contain either:
- A `table_name` column for single table analysis (adds `row_count` and `notes` columns)
- `source_table` and `target_table` columns for comparison analysis (adds row counts, notes, difference, and percentage difference columns)

**Auto-discovery:** If `tables_file` is omitted from the YAML config, dbqt will
automatically discover all tables and views in the configured database schema and
use those for row counts and/or column comparisons. In dual-database mode, only
tables present in **both** databases will have row counts collected — tables that
exist in only one side are reported with a note like *"Only in source, row count
skipped"* since comparing a missing table is not meaningful.

Table names in the CSV support flexible path formats:
- `my_table` — uses database/schema from YAML config
- `my_schema.my_table` — overrides schema, uses database from config
- `my_db.my_schema.my_table` — fully qualified, ignores config defaults

**Note:** When using dual database mode (`--source-config` and `--target-config`), the tool will:
- Connect to the source database to count rows in `source_table` column
- Connect to the target database to count rows in `target_table` column
- This enables comparing tables across different database systems (e.g., MySQL to Snowflake migration validation)

### Null Column Check Tool (dbqt nullcheck)
Check for columns where all records are null across multiple tables in Snowflake.
- Identifies completely empty columns.
- Reports on columns with low-distinct values (<=5).
- Efficiently checks multiple tables in parallel.
- Generates a markdown report summarizing the findings.

Usage:
```bash
dbqt nullcheck --config snowflake_config.yaml
```
This tool currently only supports Snowflake.

### Dynamic Query Tool (dbqt dynamic-query)
Run a dynamic SQL query against Athena for a list of values from a CSV file.
- Substitutes values from a CSV into a query template.
- Executes queries sequentially and writes results to an output file.
- Useful for running the same query against multiple tables or with different parameters.

Usage:
```bash
dbqt dynamic-query --config athena_config.yaml --csv values.csv --query "SELECT COUNT(1) FROM {var_from_csv}"
```
This tool currently only supports AWS Athena.

### Parquetizer Tool (dbqt parquetizer)
A utility to recursively find files that are Parquet but lack the `.parquet` extension and rename them.
- Scans a directory for files without extensions.
- Validates if a file is a Parquet file by checking its magic bytes.
- Renames valid Parquet files to include the `.parquet` extension.

Usage:
```bash
dbqt parquetizer [directory] # Scans from the specified directory (or current if not provided)
```

### Key Finder Tool (dbqt keyfinder)
Automatically discover composite keys (primary keys) in database tables by analyzing column combinations.
- Finds minimal composite keys (smallest column combinations that uniquely identify rows)
- Checks for NULL values (columns with NULLs cannot be part of a key)
- Prioritizes ID-like columns (columns named `id`, `*_id`, `id_*`) for faster discovery
- Supports filtering columns with `--include-only` or `--exclude` options
- Configurable maximum key size and column limits
- Parallel-safe with progress tracking
- Works with any supported database connector (Snowflake, MySQL, PostgreSQL, etc.)

Usage:
```bash
# Basic usage - find keys in a table
dbqt keyfinder --config config.yaml --table users

# Limit search to keys of size 3 or less
dbqt keyfinder --config config.yaml --table orders --max-size 3

# Exclude certain columns from the search
dbqt keyfinder --config config.yaml --table products --exclude created_at updated_at

# Only search specific columns
dbqt keyfinder --config config.yaml --table transactions --include-only user_id transaction_date store_id

# Force execution even with high combination counts
dbqt keyfinder --config config.yaml --table large_table --force

# Verbose output with progress updates
dbqt keyfinder --config config.yaml --table data --verbose
```

Example config.yaml:
```yaml
connection:
  type: snowflake
  user: myuser
  password: mypass
  account: myaccount
  database: mydb
  schema: myschema
  warehouse: mywh
  role: myrole
```

**Performance Tips:**
- The tool checks combinations starting from size 1 and stops when it finds minimal keys
- ID-like columns are checked first for faster discovery
- Use `--max-size` to limit the search space for tables with many columns
- Use `--include-only` to focus on specific columns you suspect form a key
- Use `--max-columns` to limit the number of columns analyzed (default: 20)
- The tool will warn if the combination count exceeds 50,000 (use `--force` to proceed)

## Future Plans

### Core DBQT Features (Coming Soon)
- AI-Powered column classification using Qwen2 0.5B
- Automatic check suggestions
- 20+ built-in data quality checks
- Python-first API
- No backend required
- Customizable check framework

### Planned Checks
- Completeness checks (null values)
- Uniqueness validation
- Format validation (regex, dates, emails)
- Range/boundary checks
- Value validation
- Statistical analysis
- Dependency checks

### Integration Plans
- Data pipeline integration
- Scheduled runs
- Parallel check execution
- Multiple database backend support

## 📄 License

This project is licensed under the MIT License.
