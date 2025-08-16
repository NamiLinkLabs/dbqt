import unittest
from unittest.mock import MagicMock, patch, call
import polars as pl
import threading

from dbqt.tools import dbstats


class TestDbStats(unittest.TestCase):
    def test_get_row_count_for_table_success(self):
        """Test getting row count for a table successfully."""
        mock_connector = MagicMock()
        mock_connector.count_rows.return_value = 1000

        result = dbstats.get_row_count_for_table(mock_connector, "test_table")

        self.assertEqual(result, ("test_table", (1000, None)))
        mock_connector.count_rows.assert_called_once_with("test_table")
        # Verify thread name was set
        self.assertEqual(threading.current_thread().name, "Table-test_table")

    def test_get_row_count_for_table_error(self):
        """Test getting row count when an error occurs."""
        mock_connector = MagicMock()
        mock_connector.count_rows.side_effect = Exception("Database error")

        result = dbstats.get_row_count_for_table(mock_connector, "test_table")

        self.assertEqual(result, ("test_table", (None, "Database error")))
        mock_connector.count_rows.assert_called_once_with("test_table")

    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats.load_config")
    @patch("polars.read_csv")
    def test_get_table_stats_source_target_tables(
        self, mock_read_csv, mock_load_config, mock_pool, mock_timer
    ):
        """Test get_table_stats with source_table and target_table columns."""
        # Setup mocks
        mock_config = {"tables_file": "tables.csv", "max_workers": 4}
        mock_load_config.return_value = mock_config

        # Create mock dataframe with source and target tables
        mock_df = MagicMock()
        mock_df.columns = ["source_table", "target_table"]
        mock_df.__getitem__.side_effect = lambda key: {
            "source_table": MagicMock(to_list=lambda: ["table1", "table2"]),
            "target_table": MagicMock(to_list=lambda: ["table3", "table4"]),
        }[key]

        # Create a new mock for the dataframe after with_columns
        mock_df_after_columns = MagicMock()
        mock_df_after_columns.columns = [
            "source_table",
            "target_table",
            "source_row_count",
            "source_notes",
            "target_row_count",
            "target_notes",
        ]
        mock_df_after_columns.with_columns.return_value = mock_df_after_columns
        mock_df_after_columns.select.return_value = mock_df_after_columns
        mock_df.with_columns.return_value = mock_df_after_columns

        mock_read_csv.return_value = mock_df

        # Mock connection pool
        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {
            "table1": (100, None),
            "table2": (200, None),
            "table3": (150, None),
            "table4": (250, None),
        }

        # Mock timer
        mock_timer_instance = mock_timer.return_value.__enter__.return_value

        dbstats.get_table_stats("config.yaml")

        # Verify calls
        mock_load_config.assert_called_once_with("config.yaml")
        mock_read_csv.assert_called_once_with("tables.csv")
        mock_pool.assert_called_once_with(mock_config, 4)
        mock_pool_instance.execute_parallel.assert_called_once_with(
            dbstats.get_row_count_for_table, ["table1", "table2", "table3", "table4"]
        )

        # Verify dataframe operations
        self.assertTrue(mock_df.with_columns.called)
        self.assertTrue(mock_df_after_columns.select.called)
        mock_df_after_columns.write_csv.assert_called_once_with("tables.csv")

    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats.load_config")
    @patch("polars.read_csv")
    def test_get_table_stats_single_table_column(
        self, mock_read_csv, mock_load_config, mock_pool, mock_timer
    ):
        """Test get_table_stats with table_name column."""
        # Setup mocks
        mock_config = {"tables_file": "tables.csv", "max_workers": 2}
        mock_load_config.return_value = mock_config

        # Create mock dataframe with table_name column
        mock_df = MagicMock()
        mock_df.columns = ["table_name"]
        mock_df.__getitem__.side_effect = lambda key: {
            "table_name": MagicMock(to_list=lambda: ["table1", "table2"])
        }[key]
        mock_df.with_columns.return_value = mock_df
        mock_read_csv.return_value = mock_df

        # Mock connection pool
        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {
            "table1": (100, None),
            "table2": (200, "Error message"),
        }

        dbstats.get_table_stats("config.yaml")

        # Verify calls
        mock_load_config.assert_called_once_with("config.yaml")
        mock_read_csv.assert_called_once_with("tables.csv")
        mock_pool.assert_called_once_with(mock_config, 2)
        mock_pool_instance.execute_parallel.assert_called_once_with(
            dbstats.get_row_count_for_table, ["table1", "table2"]
        )

        # Verify dataframe operations
        mock_df.with_columns.assert_called_once()
        mock_df.write_csv.assert_called_once_with("tables.csv")

    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.load_config")
    @patch("polars.read_csv")
    @patch("dbqt.tools.dbstats.logger")
    def test_get_table_stats_invalid_columns(
        self, mock_logger, mock_read_csv, mock_load_config, mock_timer
    ):
        """Test get_table_stats with invalid column structure."""
        # Setup mocks
        mock_config = {"tables_file": "tables.csv", "max_workers": 4}
        mock_load_config.return_value = mock_config

        # Create mock dataframe with invalid columns
        mock_df = MagicMock()
        mock_df.columns = ["invalid_column"]
        mock_read_csv.return_value = mock_df

        dbstats.get_table_stats("config.yaml")

        # Verify error was logged
        mock_logger.error.assert_called_once_with(
            "CSV file must contain either 'table_name' column or 'source_table' and 'target_table' columns."
        )

    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats.load_config")
    @patch("polars.read_csv")
    def test_get_table_stats_default_max_workers(
        self, mock_read_csv, mock_load_config, mock_pool, mock_timer
    ):
        """Test get_table_stats uses default max_workers when not specified."""
        # Setup mocks - no max_workers in config
        mock_config = {"tables_file": "tables.csv"}
        mock_load_config.return_value = mock_config

        # Create mock dataframe
        mock_df = MagicMock()
        mock_df.columns = ["table_name"]
        mock_df.__getitem__.side_effect = lambda key: {
            "table_name": MagicMock(to_list=lambda: ["table1"])
        }[key]
        mock_df.with_columns.return_value = mock_df
        mock_read_csv.return_value = mock_df

        # Mock connection pool
        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {"table1": (100, None)}

        dbstats.get_table_stats("config.yaml")

        # Verify default max_workers of 4 was used
        mock_pool.assert_called_once_with(mock_config, 4)

    @patch("dbqt.tools.dbstats.setup_logging")
    @patch("dbqt.tools.dbstats.get_table_stats")
    def test_main_with_args(self, mock_get_table_stats, mock_setup_logging):
        """Test main function with command line arguments."""
        args = ["--config", "test_config.yaml", "--verbose"]

        dbstats.main(args)

        mock_setup_logging.assert_called_once_with(True)
        mock_get_table_stats.assert_called_once_with("test_config.yaml")

    @patch("dbqt.tools.dbstats.setup_logging")
    @patch("dbqt.tools.dbstats.get_table_stats")
    def test_main_without_verbose(self, mock_get_table_stats, mock_setup_logging):
        """Test main function without verbose flag."""
        args = ["--config", "test_config.yaml"]

        dbstats.main(args)

        mock_setup_logging.assert_called_once_with(False)
        mock_get_table_stats.assert_called_once_with("test_config.yaml")

    @patch("dbqt.tools.dbstats.setup_logging")
    @patch("dbqt.tools.dbstats.get_table_stats")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_no_args(
        self, mock_parse_args, mock_get_table_stats, mock_setup_logging
    ):
        """Test main function when called without arguments (uses argparse)."""
        mock_args = MagicMock()
        mock_args.config = "default_config.yaml"
        mock_args.verbose = False
        mock_parse_args.return_value = mock_args

        dbstats.main(None)

        mock_parse_args.assert_called_once()
        mock_setup_logging.assert_called_once_with(False)
        mock_get_table_stats.assert_called_once_with("default_config.yaml")

    @patch("polars.Series")
    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats.load_config")
    @patch("polars.read_csv")
    def test_get_table_stats_column_operations_source_target(
        self, mock_read_csv, mock_load_config, mock_pool, mock_timer, mock_series
    ):
        """Test detailed column operations for source/target table scenario."""
        # Setup mocks
        mock_config = {"tables_file": "tables.csv", "max_workers": 4}
        mock_load_config.return_value = mock_config

        # Create mock dataframe
        mock_df = MagicMock()
        mock_df.columns = ["source_table", "target_table", "other_col"]

        # Mock the column access
        source_col_mock = MagicMock()
        source_col_mock.to_list.return_value = ["src1", "src2"]
        target_col_mock = MagicMock()
        target_col_mock.to_list.return_value = ["tgt1", "tgt2"]

        mock_df.__getitem__.side_effect = lambda key: {
            "source_table": source_col_mock,
            "target_table": target_col_mock,
        }[key]

        # Mock dataframe operations - create separate mock for after with_columns
        mock_df_with_columns = MagicMock()
        mock_df_with_columns.columns = [
            "source_table",
            "target_table",
            "other_col",
            "source_row_count",
            "source_notes",
            "target_row_count",
            "target_notes",
        ]
        mock_df_with_columns.with_columns.return_value = mock_df_with_columns
        mock_df_with_columns.select.return_value = mock_df_with_columns
        mock_df.with_columns.return_value = mock_df_with_columns

        mock_read_csv.return_value = mock_df

        # Mock connection pool
        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {
            "src1": (100, None),
            "src2": (200, "Error"),
            "tgt1": (150, None),
            "tgt2": (250, None),
        }

        # Mock polars Series creation
        mock_series.side_effect = lambda name, data: MagicMock(name=f"Series_{name}")

        dbstats.get_table_stats("config.yaml")

        # Verify Series were created with correct data
        expected_calls = [
            call("source_row_count", [100, 200]),
            call("source_notes", [None, "Error"]),
            call("target_row_count", [150, 250]),
            call("target_notes", [None, None]),
        ]
        mock_series.assert_has_calls(expected_calls, any_order=True)

    @patch("polars.Series")
    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats.load_config")
    @patch("polars.read_csv")
    def test_get_table_stats_column_operations_single_table(
        self, mock_read_csv, mock_load_config, mock_pool, mock_timer, mock_series
    ):
        """Test detailed column operations for single table scenario."""
        # Setup mocks
        mock_config = {"tables_file": "tables.csv", "max_workers": 4}
        mock_load_config.return_value = mock_config

        # Create mock dataframe
        mock_df = MagicMock()
        mock_df.columns = ["table_name"]

        # Mock the column access
        table_col_mock = MagicMock()
        table_col_mock.to_list.return_value = ["table1", "table2"]
        mock_df.__getitem__.side_effect = lambda key: {"table_name": table_col_mock}[
            key
        ]

        # Mock dataframe operations
        mock_df_with_columns = MagicMock()
        mock_df.with_columns.return_value = mock_df_with_columns

        mock_read_csv.return_value = mock_df

        # Mock connection pool
        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {
            "table1": (100, None),
            "table2": (200, "Error message"),
        }

        # Mock polars Series creation
        mock_series.side_effect = lambda name, data: MagicMock(name=f"Series_{name}")

        dbstats.get_table_stats("config.yaml")

        # Verify Series were created with correct data
        expected_calls = [
            call("row_count", [100, 200]),
            call("notes", [None, "Error message"]),
        ]
        mock_series.assert_has_calls(expected_calls, any_order=True)


if __name__ == "__main__":
    unittest.main()
