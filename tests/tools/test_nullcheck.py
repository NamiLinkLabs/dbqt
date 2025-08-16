import unittest
from unittest.mock import MagicMock, patch, mock_open

from dbqt.tools import nullcheck


class TestNullCheck(unittest.TestCase):
    def test_get_table_columns(self):
        """Test fetching table columns successfully."""
        mock_connector = MagicMock()
        mock_connector.config = {"database": "TEST_DB", "schema": "TEST_SCHEMA"}
        mock_result = MagicMock()
        mock_result.rows = [
            ("TABLE1", "COL1"),
            ("TABLE1", "COL2"),
            ("TABLE2", "COL3"),
        ]
        mock_connector.run_query.return_value = mock_result

        tables = ["TABLE1", "TABLE2"]
        expected = {"TABLE1": ["COL1", "COL2"], "TABLE2": ["COL3"]}
        result = nullcheck.get_table_columns(mock_connector, tables)
        self.assertEqual(result, expected)

    def test_get_table_columns_no_results(self):
        """Test fetching table columns when query returns no results."""
        mock_connector = MagicMock()
        mock_connector.config = {"database": "TEST_DB", "schema": "TEST_SCHEMA"}
        mock_result = MagicMock()
        mock_result.rows = []
        mock_connector.run_query.return_value = mock_result

        tables = ["TABLE1"]
        result = nullcheck.get_table_columns(mock_connector, tables)
        self.assertEqual(result, {})

    def test_check_null_columns_for_table(self):
        """Test checking null columns for a table successfully."""
        mock_connector = MagicMock()
        mock_result = MagicMock()
        mock_result.rows = [(0, 5, 1)]  # distinct counts for col1, col2, col3
        mock_connector.run_query.return_value = mock_result

        table_data = ("TABLE1", ["COL1", "COL2", "COL3"])
        expected = ("TABLE1", {"COL1": 0, "COL2": 5, "COL3": 1})
        result = nullcheck.check_null_columns_for_table(mock_connector, table_data)
        self.assertEqual(result, expected)

    def test_check_null_columns_for_table_no_columns(self):
        """Test checking a table with no columns."""
        mock_connector = MagicMock()
        table_data = ("TABLE1", [])
        expected = ("TABLE1", {})
        result = nullcheck.check_null_columns_for_table(mock_connector, table_data)
        self.assertEqual(result, expected)
        mock_connector.run_query.assert_not_called()

    def test_check_null_columns_for_table_db_error(self):
        """Test checking a table when a database error occurs."""
        mock_connector = MagicMock()
        mock_connector.run_query.side_effect = Exception("DB error")
        table_data = ("TABLE1", ["COL1"])
        expected = ("TABLE1", {})
        result = nullcheck.check_null_columns_for_table(mock_connector, table_data)
        self.assertEqual(result, expected)

    def test_write_results(self):
        """Test writing results to a file."""
        results = {
            "TABLE1": {"COL1": 0, "COL2": 10, "COL3": 3},
            "TABLE2": {"COL4": 0, "COL5": 0},
            "TABLE3": {},
            "TABLE4": {"COL6": 6},
        }
        m = mock_open()
        with patch("builtins.open", m):
            nullcheck.write_results("output.md", results)

        m.assert_called_once_with("output.md", "w")
        handle = m()

        full_text = "".join(c.args[0] for c in handle.write.call_args_list)

        expected_text = (
            "# Null Column Check Results\n\n"
            "## TABLE1\n"
            "Total columns: 3\n"
            "NULL columns (1): COL1\n"
            "Low distinct counts: {'COL3': 3}\n"
            "\n"
            "## TABLE2\n"
            "Total columns: 2\n"
            "NULL columns (2): COL4, COL5\n"
            "\n"
            "## TABLE3\n"
            "ERROR: No columns found\n\n"
            "## TABLE4\n"
            "Total columns: 1\n"
            "No NULL columns found\n"
            "\n"
            "# Summary\n"
            "Total NULL columns: 3\n"
            "NULL columns: TABLE1.COL1, TABLE2.COL4, TABLE2.COL5\n"
        )
        self.assertEqual(full_text, expected_text)

    @patch("dbqt.tools.nullcheck.get_table_columns")
    @patch("dbqt.tools.nullcheck.write_results")
    @patch("dbqt.tools.nullcheck.ConnectionPool")
    @patch("dbqt.tools.nullcheck.read_csv_list")
    @patch("dbqt.tools.nullcheck.load_config")
    @patch("dbqt.tools.nullcheck.setup_logging")
    def test_main_success(
        self,
        mock_setup_logging,
        mock_load_config,
        mock_read_csv,
        mock_pool,
        mock_write_results,
        mock_get_table_columns,
    ):
        """Test the main function for a successful run."""
        mock_load_config.return_value = {
            "connection": {"type": "Snowflake"},
            "tables_file": "tables.csv",
            "max_workers": 4,
        }
        mock_read_csv.return_value = ["Table1", "TABLE2"]
        mock_get_table_columns.return_value = {
            "TABLE1": ["COL1", "COL2"],
            "TABLE2": ["COL3"],
        }

        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        mock_pool_instance.connectors = [MagicMock()]
        mock_pool_instance.execute_parallel.return_value = {
            "Table1": {"COL1": 0, "COL2": 5},
            "TABLE2": {"COL3": 0},
        }

        args = ["--config", "config.yaml", "--output", "report.md", "--verbose"]
        return_code = nullcheck.main(args)

        mock_setup_logging.assert_called_once_with(True)
        mock_load_config.assert_called_once_with("config.yaml")
        mock_read_csv.assert_called_once_with("tables.csv", "table_name")
        mock_pool.assert_called_once_with(mock_load_config.return_value, 4)

        mock_get_table_columns.assert_called_once_with(
            mock_pool_instance.connectors[0], ["Table1", "TABLE2"]
        )

        execute_parallel_call = mock_pool_instance.execute_parallel.call_args
        self.assertEqual(
            execute_parallel_call[0][0], nullcheck.check_null_columns_for_table
        )
        self.assertEqual(
            execute_parallel_call[0][1],
            [("Table1", ["COL1", "COL2"]), ("TABLE2", ["COL3"])],
        )

        mock_write_results.assert_called_once_with(
            "report.md",
            {
                "Table1": {"COL1": 0, "COL2": 5},
                "TABLE2": {"COL3": 0},
            },
        )
        self.assertEqual(return_code, 0)

    @patch("dbqt.tools.nullcheck.setup_logging")
    @patch("dbqt.tools.nullcheck.load_config")
    def test_main_not_snowflake(self, mock_load_config, mock_setup_logging):
        """Test main function when connector type is not Snowflake."""
        mock_load_config.return_value = {"connection": {"type": "MySQL"}}
        args = ["--config", "config.yaml"]
        return_code = nullcheck.main(args)
        self.assertEqual(return_code, 1)

    @patch("dbqt.tools.nullcheck.setup_logging")
    @patch("dbqt.tools.nullcheck.load_config")
    def test_main_no_tables_file(self, mock_load_config, mock_setup_logging):
        """Test main function when no tables file is specified."""
        mock_load_config.return_value = {"connection": {"type": "Snowflake"}}
        args = ["--config", "config.yaml"]
        return_code = nullcheck.main(args)
        self.assertEqual(return_code, 1)


if __name__ == "__main__":
    unittest.main()
