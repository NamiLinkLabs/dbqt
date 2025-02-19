import os
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

def read_and_validate_schema(file_path):
    """Try to read a file as Parquet and return its schema and table if successful"""
    try:
        table = pq.read_table(file_path)
        return table.schema, table
    except Exception as e:
        return None, None

def combine_parquet_files(output_path="combined.parquet"):
    """
    Combine all readable Parquet files in the current directory into a single file.
    Skips the output file and files that can't be read as Parquet.
    """
    cwd = Path.cwd()
    files = [f for f in cwd.iterdir() if f.is_file() and f.name != output_path]
    
    if not files:
        print("No files found in the current directory")
        return
    
    # Read first valid file to get reference schema
    reference_schema = None
    tables = []
    
    print(f"Scanning {len(files)} files...")
    
    for file_path in files:
        if str(file_path).endswith('.parquet'):
            continue
        schema, table = read_and_validate_schema(file_path)
        if schema is not None:
            if reference_schema is None:
                reference_schema = schema
                tables.append(table)
                print(f"Using {file_path.name} as reference schema")
            elif schema.equals(reference_schema):
                tables.append(table)
                print(f"Added {file_path.name}")
            else:
                print(f"Skipping {file_path.name} - schema mismatch")
        else:
            print(f"Skipping {file_path.name} - not a valid Parquet file")
    
    if not tables:
        print("No valid Parquet files found")
        return
    
    # Combine tables and write output
    combined_table = pa.concat_tables(tables)
    pq.write_table(combined_table, output_path)
    print(f"\nCombined {len(tables)} files into {output_path}")
    print(f"Total rows: {len(combined_table)}")

def main(args=None):
    """Main entry point for the combine tool"""
    output_path = "combined.parquet"
    if args and len(args) > 0:
        output_path = args[0]
    
    combine_parquet_files(output_path)

if __name__ == "__main__":
    main()
