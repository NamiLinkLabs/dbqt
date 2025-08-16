# Changelog

## v0.1.12

- **Enhanced `dbstats` Tool**: Added support for source/target table comparisons with automatic difference calculations and percentage analysis.
- **Improved Error Handling**: Better error reporting throughout the application with detailed error messages and proper cleanup.
- **Robust Connection Management**: Enhanced ConnectionPool with thread-safe operations, proper connection cleanup, and comprehensive logging.
- **Comprehensive Test Coverage**: Added extensive test suites for `dbstats` and `nullcheck` modules with 100% coverage of core functionality.
- **Better Progress Tracking**: Added detailed progress logging for parallel operations to improve visibility into long-running tasks.

## v0.1.11

- **Three New Tools**: `nullcheck`, `dynamic-query`, and `parquetizer`.
- **Performance Boost for `dbstats`**: Now runs row counts in parallel, significantly speeding up execution for large numbers of tables.
- **Internal Refactoring**: Major refactoring to share code via a new `utils` module, improving consistency and maintainability.
- **Developer Experience**: Added `pre-commit` hooks for consistent code formatting.
