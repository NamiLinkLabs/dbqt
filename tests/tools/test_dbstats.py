import threading
import unittest
from unittest.mock import MagicMock, patch

import polars as pl

from dbqt.tools import dbstats


class TestDbStats(unittest.TestCase):
    def test_get_row_count_for_table_success(self):
        """Test getting row count for a table successfully."""
        mock_connector = MagicMock()
        mock_connector.count_rows.return_value = 1000

        result = dbstats.get_row_count_for_table(mock_connector, "test_table")

        self.assertEqual(result, ("test_table", (1000, None)))
        mock_connector.count_rows.assert_called_once_with("test_table")
        self.assertEqual(threading.current_thread().name, "Table-test_table")

    def test_get_row_count_for_table_error(self):
        """Test getting row count when an error occurs."""
        mock_connector = MagicMock()
        mock_connector.count_rows.side_effect = Exception("Database error")

        result = dbstats.get_row_count_for_table(mock_connector, "test_table")

        self.assertEqual(result, ("test_table", (None, "Database error")))
        mock_connector.count_rows.assert_called_once_with("test_table")

    @patch("dbqt.tools.dbstats.HTMLReport")
    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats._read_table_lists")
    @patch("dbqt.tools.dbstats.load_config")
    def test_get_table_stats_source_target_tables(
        self, mock_load_config, mock_read_tables, mock_pool, mock_timer, mock_report_cls
    ):
        """Test get_table_stats with source_table and target_table columns."""
        source_config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        target_config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        mock_load_config.side_effect = [source_config, target_config]

        df = pl.DataFrame(
            {
                "source_table": ["table1", "table2"],
                "target_table": ["table1", "table2"],
            }
        )
        mock_read_tables.return_value = (
            df,
            ["table1", "table2"],
            ["table1", "table2"],
        )

        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {
            "table1": (100, None),
            "table2": (200, None),
        }

        mock_report = mock_report_cls.return_value
        mock_report.save.return_value = "results/tables_dbstats.html"

        result = dbstats.get_table_stats(
            source_config_path="source.yaml",
            target_config_path="target.yaml",
        )

        self.assertEqual(result, "results/tables_dbstats.html")
        self.assertEqual(mock_pool.call_count, 2)
        mock_report.add_polars_tab.assert_called_once()
        mock_report.save.assert_called_once()

    @patch("dbqt.tools.dbstats.HTMLReport")
    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats._read_table_lists")
    @patch("dbqt.tools.dbstats.load_config")
    def test_get_table_stats_single_table_column(
        self, mock_load_config, mock_read_tables, mock_pool, mock_timer, mock_report_cls
    ):
        """Test get_table_stats with table_name column."""
        config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 2,
        }
        mock_load_config.return_value = config

        df = pl.DataFrame({"table_name": ["table1", "table2"]})
        mock_read_tables.return_value = (df, ["table1", "table2"], None)

        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {
            "table1": (100, None),
            "table2": (200, "Error message"),
        }

        mock_report = mock_report_cls.return_value
        mock_report.save.return_value = "results/tables_dbstats.html"

        result = dbstats.get_table_stats(config_path="config.yaml")

        self.assertEqual(result, "results/tables_dbstats.html")
        mock_pool.assert_called_once_with(config, 2)
        mock_report.add_polars_tab.assert_called_once()

    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats._read_table_lists")
    @patch("dbqt.tools.dbstats.load_config")
    def test_get_table_stats_invalid_columns(
        self, mock_load_config, mock_read_tables, mock_timer
    ):
        """Test get_table_stats with invalid column structure."""
        config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        mock_load_config.return_value = config
        mock_read_tables.side_effect = ValueError(
            "CSV must contain 'table_name' or 'source_table'/'target_table' columns."
        )

        with self.assertRaises(ValueError):
            dbstats.get_table_stats(config_path="config.yaml")

    @patch("dbqt.tools.dbstats.HTMLReport")
    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats._read_table_lists")
    @patch("dbqt.tools.dbstats.load_config")
    def test_get_table_stats_default_max_workers(
        self, mock_load_config, mock_read_tables, mock_pool, mock_timer, mock_report_cls
    ):
        """Test get_table_stats uses default max_workers when not specified."""
        config = {"connection": {"type": "mysql"}, "tables_file": "tables.csv"}
        mock_load_config.return_value = config

        df = pl.DataFrame({"table_name": ["table1"]})
        mock_read_tables.return_value = (df, ["table1"], None)

        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {"table1": (100, None)}

        mock_report = mock_report_cls.return_value
        mock_report.save.return_value = "results/tables_dbstats.html"

        dbstats.get_table_stats(config_path="config.yaml")

        mock_pool.assert_called_once_with(config, 1)

    @patch("dbqt.tools.dbstats.setup_logging")
    @patch("dbqt.tools.dbstats.get_table_stats")
    def test_main_with_args(self, mock_get_table_stats, mock_setup_logging):
        """Test main function with command line arguments."""
        args = ["rowcount", "--config", "test_config.yaml", "--verbose"]

        dbstats.main(args)

        mock_setup_logging.assert_called_once_with(True)
        mock_get_table_stats.assert_called_once_with(config_path="test_config.yaml")

    @patch("dbqt.tools.dbstats.setup_logging")
    @patch("dbqt.tools.dbstats.get_table_stats")
    def test_main_without_verbose(self, mock_get_table_stats, mock_setup_logging):
        """Test main function without verbose flag."""
        args = ["rowcount", "--config", "test_config.yaml"]

        dbstats.main(args)

        mock_setup_logging.assert_called_once_with(False)
        mock_get_table_stats.assert_called_once_with(config_path="test_config.yaml")

    @patch("dbqt.tools.dbstats.setup_logging")
    @patch("dbqt.tools.dbstats.get_table_stats")
    def test_main_no_args(self, mock_get_table_stats, mock_setup_logging):
        """Test main function when called without arguments raises SystemExit."""
        with self.assertRaises(SystemExit):
            dbstats.main([])

    @patch("dbqt.tools.dbstats.HTMLReport")
    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats._read_table_lists")
    @patch("dbqt.tools.dbstats.load_config")
    def test_get_table_stats_column_operations_source_target(
        self, mock_load_config, mock_read_tables, mock_pool, mock_timer, mock_report_cls
    ):
        """Test detailed column operations for source/target table scenario."""
        source_config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        target_config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        mock_load_config.side_effect = [source_config, target_config]

        df = pl.DataFrame(
            {
                "source_table": ["src1", "src2"],
                "target_table": ["tgt1", "tgt2"],
            }
        )
        mock_read_tables.return_value = (
            df,
            ["src1", "src2"],
            ["tgt1", "tgt2"],
        )

        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.side_effect = [
            {"src1": (100, None), "src2": (200, "Error")},
            {"tgt1": (150, None), "tgt2": (250, None)},
        ]

        mock_report = mock_report_cls.return_value
        mock_report.save.return_value = "results/tables_dbstats.html"

        result = dbstats.get_table_stats(
            source_config_path="source.yaml",
            target_config_path="target.yaml",
        )

        self.assertEqual(result, "results/tables_dbstats.html")
        # Verify the report received a Polars tab
        mock_report.add_polars_tab.assert_called_once()
        tab_name, tab_df = mock_report.add_polars_tab.call_args[0]
        self.assertEqual(tab_name, "Row Counts")
        self.assertIn("source_row_count", tab_df.columns)
        self.assertIn("target_row_count", tab_df.columns)

    @patch("dbqt.tools.dbstats.HTMLReport")
    @patch("dbqt.tools.dbstats.Timer")
    @patch("dbqt.tools.dbstats.ConnectionPool")
    @patch("dbqt.tools.dbstats._read_table_lists")
    @patch("dbqt.tools.dbstats.load_config")
    def test_get_table_stats_column_operations_single_table(
        self, mock_load_config, mock_read_tables, mock_pool, mock_timer, mock_report_cls
    ):
        """Test detailed column operations for single table scenario."""
        config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        mock_load_config.return_value = config

        df = pl.DataFrame({"table_name": ["table1", "table2"]})
        mock_read_tables.return_value = (df, ["table1", "table2"], None)

        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.execute_parallel.return_value = {
            "table1": (100, None),
            "table2": (200, "Error message"),
        }

        mock_report = mock_report_cls.return_value
        mock_report.save.return_value = "results/tables_dbstats.html"

        result = dbstats.get_table_stats(config_path="config.yaml")

        self.assertEqual(result, "results/tables_dbstats.html")
        mock_report.add_polars_tab.assert_called_once()
        tab_name, tab_df = mock_report.add_polars_tab.call_args[0]
        self.assertEqual(tab_name, "Row Counts")
        self.assertIn("row_count", tab_df.columns)
        self.assertIn("notes", tab_df.columns)
        # Verify actual data
        self.assertEqual(tab_df["row_count"].to_list(), [100, 200])
        self.assertEqual(tab_df["notes"].to_list(), [None, "Error message"])

    @patch("dbqt.tools.dbstats.HTMLReport")
    @patch("dbqt.tools.colcompare.colcompare_from_db")
    @patch("dbqt.tools.dbstats._read_table_lists")
    @patch("dbqt.tools.dbstats.load_config")
    @patch("dbqt.tools.dbstats.setup_logging")
    def test_main_both_mode_reuses_configs_and_tables(
        self,
        mock_setup_logging,
        mock_load_config,
        mock_read_tables,
        mock_colcompare,
        mock_report_cls,
    ):
        """Test that 'both' mode loads configs and discovers tables only once."""
        source_config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        target_config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        mock_load_config.side_effect = [source_config, target_config]

        df = pl.DataFrame(
            {
                "source_table": ["table1", "table2"],
                "target_table": ["table1", "table2"],
            }
        )
        table_lists = (df, ["table1", "table2"], ["table1", "table2"])
        mock_read_tables.return_value = table_lists

        mock_report = mock_report_cls.return_value
        mock_report.save.return_value = "results/tables_dbstats.html"

        with (
            patch("dbqt.tools.dbstats.ConnectionPool") as mock_pool,
            patch("dbqt.tools.dbstats.Timer"),
        ):
            mock_pool_instance = mock_pool.return_value.__enter__.return_value
            mock_pool_instance.execute_parallel.return_value = {
                "table1": (100, None),
                "table2": (200, None),
            }

            dbstats.main(
                [
                    "both",
                    "--source-config",
                    "source.yaml",
                    "--target-config",
                    "target.yaml",
                ]
            )

        # load_config called exactly twice (once per config file)
        self.assertEqual(mock_load_config.call_count, 2)

        # _read_table_lists called exactly once (shared between rowcount and colcompare)
        mock_read_tables.assert_called_once()

        # colcompare_from_db received pre-loaded configs and table lists
        mock_colcompare.assert_called_once()
        call_kwargs = mock_colcompare.call_args[1]
        self.assertIs(call_kwargs["source_config"], source_config)
        self.assertIs(call_kwargs["target_config"], target_config)
        self.assertEqual(call_kwargs["source_tables"], ["table1", "table2"])
        self.assertEqual(call_kwargs["target_tables"], ["table1", "table2"])

    @patch("dbqt.tools.dbstats.HTMLReport")
    @patch("dbqt.tools.colcompare.colcompare_from_db")
    @patch("dbqt.tools.dbstats._read_table_lists")
    @patch("dbqt.tools.dbstats.load_config")
    @patch("dbqt.tools.dbstats.setup_logging")
    def test_main_both_mode_merges_excluded_cols_from_configs(
        self,
        mock_setup_logging,
        mock_load_config,
        mock_read_tables,
        mock_colcompare,
        mock_report_cls,
    ):
        """Test that 'both' mode merges excluded_cols from source and target configs."""
        source_config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
            "excluded_cols": ["CREATED_AT"],
        }
        target_config = {
            "connection": {"type": "mysql"},
            "tables_file": "tables.csv",
            "max_workers": 4,
            "excluded_cols": ["UPDATED_AT"],
        }
        mock_load_config.side_effect = [source_config, target_config]

        df = pl.DataFrame(
            {
                "source_table": ["table1"],
                "target_table": ["table1"],
            }
        )
        table_lists = (df, ["table1"], ["table1"])
        mock_read_tables.return_value = table_lists

        mock_report = mock_report_cls.return_value
        mock_report.save.return_value = "results/tables_dbstats.html"

        with (
            patch("dbqt.tools.dbstats.ConnectionPool") as mock_pool,
            patch("dbqt.tools.dbstats.Timer"),
        ):
            mock_pool_instance = mock_pool.return_value.__enter__.return_value
            mock_pool_instance.execute_parallel.return_value = {
                "table1": (100, None),
            }

            dbstats.main(
                [
                    "both",
                    "--source-config",
                    "source.yaml",
                    "--target-config",
                    "target.yaml",
                ]
            )

        mock_colcompare.assert_called_once()
        call_kwargs = mock_colcompare.call_args[1]
        self.assertEqual(call_kwargs["excluded_cols"], {"CREATED_AT", "UPDATED_AT"})


if __name__ == "__main__":
    unittest.main()
