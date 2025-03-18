import os
import argparse
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

def combine_parquet_files(output_path="combined.parquet", delete_original=False):
    """
    Combine all readable Parquet files in the current directory and subdirectories.
    For subdirectories containing Parquet files, combines them into a file named after the directory.
    
    Args:
        output_path: Default output path for files in the root directory
        delete_original: If True, deletes original files after successful combination
    """
    cwd = Path.cwd()
    
    # First, handle subdirectories
    subdirs = [d for d in cwd.iterdir() if d.is_dir()]
    for subdir in subdirs:
        files = [f for f in subdir.iterdir() if f.is_file() and f.name.endswith('.parquet')]
        if files:
            # Use directory name as output filename
            subdir_output = subdir / f"{subdir.name}.parquet"
            _combine_files(files, subdir_output, delete_original)
    
    # Then handle files in root directory
    root_files = [f for f in cwd.iterdir() if f.is_file() and f.name.endswith('.parquet')]
    if root_files:
        _combine_files(root_files, output_path, delete_original)
    
def _combine_files(files, output_path: str | Path, delete_original=False):
    """Helper function to combine a list of Parquet files"""
    output_path = Path(output_path)
    if not files or output_path in files:
        return

    # Read first valid file to get reference schema
    reference_schema = None
    tables = []
    files_to_delete = []
    
    print(f"\nScanning {len(files)} files in {files[0].parent}...")
    
    for file_path in files:
        output_path = Path(output_path)
        if file_path.name == output_path.name:
            continue
            
        schema, table = read_and_validate_schema(file_path)
        if schema is not None:
            if reference_schema is None:
                reference_schema = schema
                tables.append(table)
                files_to_delete.append(file_path)
                print(f"Using {file_path.name} as reference schema")
            elif schema.equals(reference_schema):
                tables.append(table)
                files_to_delete.append(file_path)
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
    
    # Delete original files if requested
    if delete_original:
        for file_path in files_to_delete:
            file_path.unlink()
            print(f"Deleted {file_path}")

def main(args=None):
    """Main entry point for the combine tool"""
    parser = argparse.ArgumentParser(
        description='Combine multiple Parquet files in the current directory and subdirectories',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scans the current directory and subdirectories for Parquet files and combines them if they share
the same schema. Files with different schemas are skipped.

For files in subdirectories, the combined output is named after the directory:
./subdir/file1.parquet + ./subdir/file2.parquet -> ./subdir/subdir.parquet

For files in the root directory, the output is written to the specified output file
(defaults to combined.parquet).
        """
    )
    parser.add_argument('output', nargs='?', default='combined.parquet',
                       help='Output filename for root directory files (default: combined.parquet)')
    parser.add_argument('--delete-original', action='store_true',
                       help='Delete original files after successful combination')
    
    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)
    
    combine_parquet_files(args.output, args.delete_original)

if __name__ == "__main__":
    main()
