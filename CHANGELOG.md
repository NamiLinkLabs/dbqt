# Changelog

## v0.1.11

- **Three New Tools**: `nullcheck`, `dynamic-query`, and `parquetizer`.
- **Performance Boost for `dbstats`**: Now runs row counts in parallel, significantly speeding up execution for large numbers of tables.
- **Internal Refactoring**: Major refactoring to share code via a new `utils` module, improving consistency and maintainability.
- **Developer Experience**: Added `pre-commit` hooks for consistent code formatting.
