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


def combine_parquet_files(
    output_path="combined.parquet", delete_original=False, one_file=False
):
    """
    Combine all readable Parquet files in the current directory and subdirectories.

    If one_file is True, combines all files into a single output file in the working directory.
    Otherwise, maintains backward compatibility by combining files within their own folders.

    Args:
        output_path: Output path for the combined file
        delete_original: If True, deletes original files after successful combination
        one_file: If True, combines all files into single output in current working directory
    """
    cwd = Path.cwd()

    if one_file:
        # New behavior: combine all files into one output in current working directory
        all_files = []

        # Collect all Parquet files from current directory and subdirectories
        for root, dirs, files in os.walk(cwd):
            root_path = Path(root)
            parquet_files = [root_path / f for f in files if f.endswith(".parquet")]
            all_files.extend(parquet_files)

        if all_files:
            output_path = cwd / Path(output_path).name
            _combine_files(all_files, output_path, delete_original)
        else:
            print("No Parquet files found in current directory or subdirectories")
    else:
        # Original behavior: combine files within their own folders
        # First, handle subdirectories
        subdirs = [d for d in cwd.iterdir() if d.is_dir()]
        for subdir in subdirs:
            files = [
                f
                for f in subdir.iterdir()
                if f.is_file() and f.name.endswith(".parquet")
            ]
            if files:
                # Use directory name as output filename
                subdir_output = subdir / f"{subdir.name}.parquet"
                _combine_files(files, subdir_output, delete_original)

        # Then handle files in root directory
        root_files = [
            f for f in cwd.iterdir() if f.is_file() and f.name.endswith(".parquet")
        ]
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
        description="Combine multiple Parquet files in the current directory and subdirectories",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scans the current directory and subdirectories for Parquet files and combines them if they share
the same schema. Files with different schemas are skipped.

By default, files in subdirectories are combined into files named after their directory.
Use --one-file to combine all files into a single output file in the working directory.
        """,
    )
    parser.add_argument(
        "output",
        nargs="?",
        default="combined.parquet",
        help="Output filename for combined file (default: combined.parquet)",
    )
    parser.add_argument(
        "--delete-original",
        action="store_true",
        help="Delete original files after successful combination",
    )
    parser.add_argument(
        "--one-file",
        action="store_true",
        help="Force output file to be created in current working directory",
    )

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    combine_parquet_files(args.output, args.delete_original, args.one_file)


if __name__ == "__main__":
    main()
