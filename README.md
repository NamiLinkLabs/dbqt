# DBQT (DataBase Quality Tool) 🎯

DBQT is a lightweight, Python-first data quality testing framework that helps data teams maintain high-quality data through automated checks and intelligent suggestions. 

## 🛠️ Current Tools

### Column Comparison Tool (dbqt compare)
Compare schemas between databases or files:
- Table-level comparison
- Column-level comparison with data type compatibility checks
- Support for CSV and Parquet files
- Handles nested Parquet schemas (arrays, structs, maps)
- Intelligent data type compatibility checking
- Generates detailed Excel report with:
  - Table differences
  - Column differences
  - Data type mismatches
  - Formatted worksheets for easy analysis

Usage:
```bash
dbqt compare source_schema.csv target_schema.csv
# Or compare Parquet files directly:
dbqt compare source.parquet target.parquet
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

### Database Statistics Tool (dbqt dbstats)
Collect and analyze database statistics:
- Fetches table row counts in parallel for faster execution
- Supports both single table analysis and source/target table comparisons
- Automatically calculates differences and percentage changes for comparisons
- Updates statistics in a CSV file with comprehensive error reporting
- Configurable through YAML

Usage:
```bash
dbqt dbstats config.yaml
```

Example config.yaml:
```yaml
# Database connection configuration
connection:
  type: mysql  # mysql, snowflake, duckdb, csv, parquet, s3parquet
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
```

The tables.csv file should contain either:
- A `table_name` column for single table analysis (adds `row_count` and `notes` columns)
- `source_table` and `target_table` columns for comparison analysis (adds row counts, notes, difference, and percentage difference columns)

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

## 🚀 Future Plans

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
