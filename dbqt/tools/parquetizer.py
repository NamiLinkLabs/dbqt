import os
import argparse
from pathlib import Path

def add_parquet_extension(path):
    """
    Recursively find files without extensions (excluding hidden files) 
    and add .parquet extension
    """
    path = Path(path)
    count = 0
    
    for item in path.rglob('*'):
        # Skip directories and hidden files
        if item.is_dir() or item.name.startswith('.'):
            continue
            
        # If file has no suffix, add .parquet
        if not item.suffix:
            new_name = item.with_suffix('.parquet')
            item.rename(new_name)
            print(f"Renamed: {item} -> {new_name}")
            count += 1
    
    if count > 0:
        print(f"\nAdded .parquet extension to {count} files")
    else:
        print("No files found requiring .parquet extension")

def main(args=None):
    """Main entry point for the parquetizer tool"""
    parser = argparse.ArgumentParser(
        description='Add .parquet extension to files without extensions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Recursively searches the current directory for files without extensions
(excluding hidden files) and adds .parquet extension to them.

Example:
  ./data/file1 -> ./data/file1.parquet
  ./data/file2 -> ./data/file2.parquet
        """
    )
    parser.add_argument('directory', nargs='?', default='.',
                       help='Directory to process (default: current directory)')
    
    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)
    
    add_parquet_extension(args.directory)

if __name__ == "__main__":
    main()
