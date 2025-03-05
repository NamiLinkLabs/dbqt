import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: dbqt <command> [args...]")
        print("Commands:")
        print("  colcompare <source.csv> <target.csv>")
        print("  dbstats <config.yaml>")
        print("  combine [combined.parquet]")
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
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
