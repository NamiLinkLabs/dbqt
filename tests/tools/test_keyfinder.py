import unittest
from unittest.mock import MagicMock, patch, call
from dbqt.tools import keyfinder


class TestKeyFinder(unittest.TestCase):
    def test_get_column_names(self):
        """Test fetching column names successfully."""
        mock_connector = MagicMock()
        mock_connector.fetch_table_metadata.return_value = [
            ("COL1", "VARCHAR"),
            ("COL2", "INTEGER"),
            ("COL3", "DATE"),
        ]

        result = keyfinder.get_column_names(mock_connector, "test_table")

        self.assertEqual(result, ["COL1", "COL2", "COL3"])
        mock_connector.fetch_table_metadata.assert_called_once_with("test_table")

    def test_get_row_count(self):
        """Test getting row count successfully."""
        mock_connector = MagicMock()
        mock_connector.count_rows.return_value = 1000

        result = keyfinder.get_row_count(mock_connector, "test_table")

        self.assertEqual(result, 1000)
        mock_connector.count_rows.assert_called_once_with("test_table")

    def test_check_key_candidate_valid(self):
        """Test checking a valid key candidate."""
        mock_connector = MagicMock()
        # First call checks for NULLs (returns 0), second call checks distinct count
        mock_connector.run_query.side_effect = [[[0]], [[1000]]]

        result = keyfinder.check_key_candidate(
            mock_connector, "test_table", ("COL1", "COL2"), 1000
        )

        self.assertTrue(result)

    def test_check_key_candidate_invalid(self):
        """Test checking an invalid key candidate."""
        mock_connector = MagicMock()
        # First call checks for NULLs (returns 0), second call checks distinct count
        mock_connector.run_query.side_effect = [[[0]], [[500]]]

        result = keyfinder.check_key_candidate(
            mock_connector, "test_table", ("COL1",), 1000
        )

        self.assertFalse(result)

    def test_check_key_candidate_with_nulls(self):
        """Test checking a key candidate that contains NULL values."""
        mock_connector = MagicMock()
        # First call checks for NULLs (returns 10 rows with NULLs)
        mock_connector.run_query.return_value = [[10]]

        result = keyfinder.check_key_candidate(
            mock_connector, "test_table", ("COL1", "COL2"), 1000
        )

        # Should return False because columns contain NULLs
        self.assertFalse(result)

        # Should only call run_query once (for NULL check), not twice
        self.assertEqual(mock_connector.run_query.call_count, 1)

        # Verify the NULL check query was executed
        call_args = mock_connector.run_query.call_args[0][0]
        self.assertIn("IS NULL", call_args)
        self.assertIn("COL1", call_args)
        self.assertIn("COL2", call_args)

    def test_calculate_total_combinations(self):
        """Test calculating total combinations."""
        # 3 columns, all sizes: 2^3 - 1 = 7
        self.assertEqual(keyfinder.calculate_total_combinations(3), 7)

        # 3 columns, max size 2: C(3,1) + C(3,2) = 3 + 3 = 6
        self.assertEqual(keyfinder.calculate_total_combinations(3, 2), 6)

        # 10 columns, all sizes: 2^10 - 1 = 1023
        self.assertEqual(keyfinder.calculate_total_combinations(10), 1023)

    @patch("dbqt.tools.keyfinder.check_key_candidate")
    @patch("dbqt.tools.keyfinder.get_row_count")
    def test_find_composite_keys_single_column(
        self, mock_get_row_count, mock_check_key
    ):
        """Test finding a single column key."""
        mock_connector = MagicMock()
        mock_get_row_count.return_value = 1000

        # First column is a key
        mock_check_key.side_effect = [True, False, False]

        result = keyfinder.find_composite_keys(
            mock_connector, "test_table", ["COL1", "COL2", "COL3"]
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], ("COL1",))

    @patch("dbqt.tools.keyfinder.check_key_candidate")
    @patch("dbqt.tools.keyfinder.get_row_count")
    def test_find_composite_keys_two_columns(self, mock_get_row_count, mock_check_key):
        """Test finding a two-column composite key."""
        mock_connector = MagicMock()
        mock_get_row_count.return_value = 1000

        # No single column is a key, but COL1+COL2 is
        mock_check_key.side_effect = [False, False, False, True]

        result = keyfinder.find_composite_keys(
            mock_connector, "test_table", ["COL1", "COL2", "COL3"]
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], ("COL1", "COL2"))

    @patch("dbqt.tools.keyfinder.check_key_candidate")
    @patch("dbqt.tools.keyfinder.get_row_count")
    def test_find_composite_keys_empty_table(self, mock_get_row_count, mock_check_key):
        """Test handling empty table."""
        mock_connector = MagicMock()
        mock_get_row_count.return_value = 0

        result = keyfinder.find_composite_keys(
            mock_connector, "test_table", ["COL1", "COL2"]
        )

        self.assertEqual(result, [])
        mock_check_key.assert_not_called()

    @patch("dbqt.tools.keyfinder.logger")
    @patch("dbqt.tools.keyfinder.check_key_candidate")
    @patch("dbqt.tools.keyfinder.get_row_count")
    def test_find_composite_keys_with_id_columns(
        self, mock_get_row_count, mock_check_key, mock_logger
    ):
        """Test that ID columns are prioritized and logged."""
        mock_connector = MagicMock()
        mock_get_row_count.return_value = 1000

        # user_id is an ID column, should be checked first
        columns = ["name", "user_id", "email"]

        # user_id is a valid key
        mock_check_key.side_effect = [True]

        result = keyfinder.find_composite_keys(mock_connector, "test_table", columns)

        # Verify ID column logging
        mock_logger.info.assert_any_call(
            "Found 1 ID-like column(s), checking those first"
        )

        # Verify user_id was checked first (it's the key)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], ("user_id",))

        # Verify user_id was the first column checked
        first_call = mock_check_key.call_args_list[0]
        self.assertEqual(first_call[0][2], ("user_id",))

    @patch("dbqt.tools.keyfinder.check_key_candidate")
    @patch("dbqt.tools.keyfinder.get_row_count")
    def test_find_composite_keys_skips_supersets(
        self, mock_get_row_count, mock_check_key
    ):
        """Test that supersets of found keys are skipped."""
        mock_connector = MagicMock()
        mock_get_row_count.return_value = 1000

        # COL1 is a key, so (COL1, COL2) and (COL1, COL3) should be skipped
        mock_check_key.side_effect = [True, False, False]

        result = keyfinder.find_composite_keys(
            mock_connector, "test_table", ["COL1", "COL2", "COL3"]
        )

        # Should only find COL1 and stop checking size 2 combinations
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], ("COL1",))

        # Should only check 3 single columns, not the 3 two-column combinations
        self.assertEqual(mock_check_key.call_count, 3)

    @patch("dbqt.tools.keyfinder.logger")
    @patch("dbqt.tools.keyfinder.check_key_candidate")
    @patch("dbqt.tools.keyfinder.get_row_count")
    def test_find_composite_keys_verbose_progress(
        self, mock_get_row_count, mock_check_key, mock_logger
    ):
        """Test that verbose mode logs progress every 100 combinations."""
        mock_connector = MagicMock()
        mock_get_row_count.return_value = 1000

        # Create enough columns to generate >100 combinations at size 2
        # 15 columns = C(15,2) = 105 combinations
        columns = [f"COL{i}" for i in range(15)]

        # No keys found (all return False)
        mock_check_key.return_value = False

        result = keyfinder.find_composite_keys(
            mock_connector, "test_table", columns, verbose=True
        )

        # Should log progress at 100th combination
        progress_calls = [
            call for call in mock_logger.info.call_args_list if "Progress:" in str(call)
        ]
        self.assertGreater(len(progress_calls), 0)

        # Verify progress message format
        self.assertTrue(any("100/105" in str(call) for call in progress_calls))

    @patch("dbqt.tools.keyfinder.Timer")
    @patch("dbqt.tools.keyfinder.create_connector")
    @patch("dbqt.tools.keyfinder.load_config")
    @patch("dbqt.tools.keyfinder.find_composite_keys")
    @patch("dbqt.tools.keyfinder.get_column_names")
    def test_keyfinder_success(
        self,
        mock_get_columns,
        mock_find_keys,
        mock_load_config,
        mock_create_connector,
        mock_timer,
    ):
        """Test successful keyfinder execution."""
        mock_config = {"connection": {"type": "Snowflake"}}
        mock_load_config.return_value = mock_config

        mock_connector = MagicMock()
        mock_create_connector.return_value = mock_connector

        mock_get_columns.return_value = ["COL1", "COL2", "COL3"]
        mock_find_keys.return_value = [("COL1", "COL2")]

        keyfinder.keyfinder(
            config_path="config.yaml",
            table_name="test_table",
            max_key_size=3,
            verbose=False,
        )

        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()
        mock_find_keys.assert_called_once()

    @patch("dbqt.tools.keyfinder.Timer")
    @patch("dbqt.tools.keyfinder.create_connector")
    @patch("dbqt.tools.keyfinder.load_config")
    @patch("dbqt.tools.keyfinder.get_column_names")
    def test_keyfinder_too_many_combinations(
        self,
        mock_get_columns,
        mock_load_config,
        mock_create_connector,
        mock_timer,
    ):
        """Test handling too many combinations without force flag."""
        mock_config = {"connection": {"type": "Snowflake"}}
        mock_load_config.return_value = mock_config

        mock_connector = MagicMock()
        mock_create_connector.return_value = mock_connector

        # 20 columns = 1,048,575 combinations
        mock_get_columns.return_value = [f"COL{i}" for i in range(20)]

        keyfinder.keyfinder(
            config_path="config.yaml",
            table_name="test_table",
            force=False,
            verbose=False,
        )

        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()

    @patch("dbqt.tools.keyfinder.setup_logging")
    @patch("dbqt.tools.keyfinder.keyfinder")
    def test_main_with_args(self, mock_keyfinder, mock_setup_logging):
        """Test main function with arguments."""
        args = [
            "--config",
            "config.yaml",
            "--table",
            "test_table",
            "--max-size",
            "3",
            "--verbose",
        ]

        keyfinder.main(args)

        mock_setup_logging.assert_called_once_with(True)
        mock_keyfinder.assert_called_once()

    @patch("dbqt.tools.keyfinder.setup_logging")
    @patch("dbqt.tools.keyfinder.keyfinder")
    def test_main_with_filters(self, mock_keyfinder, mock_setup_logging):
        """Test main function with column filters."""
        args = [
            "--config",
            "config.yaml",
            "--table",
            "test_table",
            "--exclude",
            "id",
            "created_at",
            "--include-only",
            "col1",
            "col2",
            "col3",
        ]

        keyfinder.main(args)

        mock_keyfinder.assert_called_once()
        call_kwargs = mock_keyfinder.call_args[1]
        self.assertEqual(call_kwargs["exclude_columns"], ["id", "created_at"])
        self.assertEqual(call_kwargs["include_columns"], ["col1", "col2", "col3"])

    @patch("dbqt.tools.keyfinder.logger")
    @patch("dbqt.tools.keyfinder.Timer")
    @patch("dbqt.tools.keyfinder.create_connector")
    @patch("dbqt.tools.keyfinder.load_config")
    @patch("dbqt.tools.keyfinder.get_column_names")
    def test_keyfinder_no_columns_found(
        self,
        mock_get_columns,
        mock_load_config,
        mock_create_connector,
        mock_timer,
        mock_logger,
    ):
        """Test handling when no columns are found for table."""
        mock_config = {"connection": {"type": "Snowflake"}}
        mock_load_config.return_value = mock_config

        mock_connector = MagicMock()
        mock_create_connector.return_value = mock_connector

        # Return empty list (no columns)
        mock_get_columns.return_value = []

        keyfinder.keyfinder(
            config_path="config.yaml", table_name="test_table", verbose=False
        )

        mock_logger.error.assert_called_with("No columns found for table test_table")
        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()

    @patch("dbqt.tools.keyfinder.logger")
    @patch("dbqt.tools.keyfinder.Timer")
    @patch("dbqt.tools.keyfinder.create_connector")
    @patch("dbqt.tools.keyfinder.load_config")
    @patch("dbqt.tools.keyfinder.get_column_names")
    def test_keyfinder_no_columns_after_include_filter(
        self,
        mock_get_columns,
        mock_load_config,
        mock_create_connector,
        mock_timer,
        mock_logger,
    ):
        """Test handling when no columns remain after include filter."""
        mock_config = {"connection": {"type": "Snowflake"}}
        mock_load_config.return_value = mock_config

        mock_connector = MagicMock()
        mock_create_connector.return_value = mock_connector

        mock_get_columns.return_value = ["COL1", "COL2", "COL3"]

        # Include columns that don't exist
        keyfinder.keyfinder(
            config_path="config.yaml",
            table_name="test_table",
            include_columns=["NONEXISTENT1", "NONEXISTENT2"],
            verbose=False,
        )

        mock_logger.error.assert_called_with("No columns remaining after filters")
        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()

    @patch("dbqt.tools.keyfinder.logger")
    @patch("dbqt.tools.keyfinder.Timer")
    @patch("dbqt.tools.keyfinder.create_connector")
    @patch("dbqt.tools.keyfinder.load_config")
    @patch("dbqt.tools.keyfinder.get_column_names")
    def test_keyfinder_no_columns_after_exclude_filter(
        self,
        mock_get_columns,
        mock_load_config,
        mock_create_connector,
        mock_timer,
        mock_logger,
    ):
        """Test handling when no columns remain after exclude filter."""
        mock_config = {"connection": {"type": "Snowflake"}}
        mock_load_config.return_value = mock_config

        mock_connector = MagicMock()
        mock_create_connector.return_value = mock_connector

        mock_get_columns.return_value = ["COL1", "COL2", "COL3"]

        # Exclude all columns
        keyfinder.keyfinder(
            config_path="config.yaml",
            table_name="test_table",
            exclude_columns=["COL1", "COL2", "COL3"],
            verbose=False,
        )

        mock_logger.error.assert_called_with("No columns remaining after filters")
        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()

    @patch("dbqt.tools.keyfinder.logger")
    @patch("dbqt.tools.keyfinder.Timer")
    @patch("dbqt.tools.keyfinder.create_connector")
    @patch("dbqt.tools.keyfinder.load_config")
    @patch("dbqt.tools.keyfinder.get_column_names")
    @patch("dbqt.tools.keyfinder.find_composite_keys")
    def test_keyfinder_exceeds_max_columns(
        self,
        mock_find_keys,
        mock_get_columns,
        mock_load_config,
        mock_create_connector,
        mock_timer,
        mock_logger,
    ):
        """Test handling when table has more columns than max_columns limit."""
        mock_config = {"connection": {"type": "Snowflake"}}
        mock_load_config.return_value = mock_config

        mock_connector = MagicMock()
        mock_create_connector.return_value = mock_connector

        # Return 25 columns
        mock_get_columns.return_value = [f"COL{i}" for i in range(25)]
        mock_find_keys.return_value = [("COL1",)]

        # Set max_columns to 10
        keyfinder.keyfinder(
            config_path="config.yaml",
            table_name="test_table",
            max_columns=10,
            verbose=False,
        )

        # Verify warning was logged
        mock_logger.warning.assert_called_with(
            "Table has 25 columns, limiting to first 10. "
            "Use --max-columns to adjust or --include-only to specify columns"
        )

        # Verify find_composite_keys was called with only first 10 columns
        call_args = mock_find_keys.call_args[0]
        columns_passed = call_args[2]
        self.assertEqual(len(columns_passed), 10)
        self.assertEqual(columns_passed, [f"COL{i}" for i in range(10)])

        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()


if __name__ == "__main__":
    unittest.main()
