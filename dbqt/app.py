import sys


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ['-h', '--help']:
        print("""
Database Query Tools (dbqt)

Usage: dbqt <command> [args...]

Commands:
  colcompare   Compare column schemas between CSV/Parquet files
  dbstats      Get row counts for database tables
  combine      Combine multiple Parquet files with matching schemas
  parquetizer  Add .parquet extension to files without extensions

Run 'dbqt <command> --help' for detailed help on each command.
        """.strip())
        sys.exit(1)

    command = sys.argv[1]
    args = sys.argv[2:]

    if command in ["colcompare", "compare"]:
        from dbqt.tools import colcompare
        colcompare.colcompare(args)
    elif command in ["dbstats", "rowcount"]:
        from dbqt.tools import dbstats
        dbstats.main(args)
    elif command in ["combine"]:
        from dbqt.tools import combine
        combine.main(args)
    elif command in ["parquetizer"]:
        from dbqt.tools import parquetizer
        parquetizer.main(args)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
