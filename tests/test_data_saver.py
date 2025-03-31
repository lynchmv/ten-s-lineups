import unittest
from unittest.mock import patch, mock_open, MagicMock
import pandas as pd
import json
import os
import logging
from src.processing.data_saver import _save_parquet, _save_json, save_player_profile, _extract_results_data, validate_results_data, save_player_results, save_player_profile

class TestDataSaverSaveParquet(unittest.TestCase):

    @patch('pandas.DataFrame.to_parquet')
    @patch('src.processing.data_saver.logger')  # Patch the logger instance directly
    def test_save_parquet_success(self, mock_logger, mock_to_parquet):
        """Test successful saving of DataFrame to Parquet."""
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        file_path = "test_path.parquet"

        _save_parquet(df, file_path)

        mock_to_parquet.assert_called_once_with(file_path, engine="pyarrow", index=False)
        mock_logger.debug.assert_any_call(f"Saving DataFrame with {len(df)} rows to {file_path}")
        mock_logger.debug.assert_any_call(f"Successfully saved DataFrame to {file_path}")
        mock_logger.error.assert_not_called()

    @patch('pandas.DataFrame.to_parquet')
    @patch('src.processing.data_saver.logger')  # Patch the logger instance directly
    def test_save_parquet_error(self, mock_logger, mock_to_parquet):
        """Test error during saving of DataFrame to Parquet."""
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        file_path = "test_path.parquet"
        error_message = "Disk full"
        mock_to_parquet.side_effect = Exception(error_message)

        _save_parquet(df, file_path)

        mock_to_parquet.assert_called_once_with(file_path, engine="pyarrow", index=False)
        mock_logger.debug.assert_any_call(f"Saving DataFrame with {len(df)} rows to {file_path}")
        mock_logger.error.assert_called_once_with(f"Error saving DataFrame to {file_path}: {error_message}")

    pass

class TestDataSaverSaveJson(unittest.TestCase):

    @patch('json.dump')
    @patch('builtins.open', new_callable=mock_open)
    @patch('src.processing.data_saver.logger')
    def test_save_json_success(self, mock_logger, mock_open_file, mock_json_dump):
        """Test successful saving of data to JSON."""
        contents = {"key": "value"}
        file_path = "test_path.json"

        _save_json(contents, file_path)

        mock_open_file.assert_called_once_with(file_path, "w")
        mock_json_dump.assert_called_once_with(contents, mock_open_file.return_value, indent=2)
        mock_logger.debug.assert_any_call(f"Saving JSON data to {file_path}")
        mock_logger.debug.assert_any_call(f"Successfully saved raw json to {file_path}")
        mock_logger.error.assert_not_called()

    @patch('json.dump')
    @patch('builtins.open', new_callable=mock_open)
    @patch('src.processing.data_saver.logger')
    def test_save_json_error(self, mock_logger, mock_open_file, mock_json_dump):
        """Test error during saving of data to JSON."""
        contents = {"key": "value"}
        file_path = "test_path.json"
        error_message = "Permission denied"
        mock_open_file.side_effect = IOError(error_message)

        _save_json(contents, file_path)

        mock_open_file.assert_called_once_with(file_path, "w")
        mock_json_dump.assert_not_called()
        mock_logger.debug.assert_any_call(f"Saving JSON data to {file_path}")
        mock_logger.error.assert_called_once_with(f"Error saving raw json to {file_path}: {error_message}")

    pass

class TestDataSaverSavePlayerProfile(unittest.TestCase):

    @patch('src.processing.data_saver.logger')
    @patch('pandas.json_normalize')
    @patch('os.makedirs')
    @patch('os.path.join')
    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    def test_save_player_profile_success(self, mock_save_parquet, mock_save_json, mock_os_path_join, mock_makedirs, mock_json_normalize, mock_logger):
        """Test successful saving of player profile to Parquet and JSON."""
        expected_df = pd.DataFrame({'col1': [1], 'col2': [2]})
        mock_json_normalize.return_value = expected_df
        mock_os_path_join.side_effect = ["data/processed/player_test_profile.parquet", "data/raw/player_test_profile.json"]

        profile = {"name": "Test Player", "level": 10}
        player_id = "test_player"

        save_player_profile(profile, player_id)

        mock_logger.info.assert_any_call(f"Saving profile for player ID: {player_id}.")
        mock_json_normalize.assert_called_once_with(profile)
        mock_makedirs.assert_any_call("data/processed", exist_ok=True)
        mock_os_path_join.assert_any_call("data/processed", f"player_{player_id}_profile.parquet")
        mock_logger.info.assert_any_call(f"Saving {player_id} profile to Parquet file.")
        mock_save_parquet.assert_called_once()
        call_args_parquet = mock_save_parquet.call_args[0]
        self.assertTrue(call_args_parquet[0].equals(expected_df))
        self.assertEqual(call_args_parquet[1], "data/processed/player_test_profile.parquet")
        mock_makedirs.assert_any_call("data/raw", exist_ok=True)
        mock_os_path_join.assert_any_call("data/raw", f"player_{player_id}_profile.json")
        mock_logger.info.assert_any_call(f"Saving {player_id} profile to json file.")
        mock_save_json.assert_called_once_with(profile, "data/raw/player_test_profile.json")
        mock_logger.error.assert_not_called()

    @patch('src.processing.data_saver.logger')
    @patch('pandas.json_normalize')
    @patch('os.makedirs')
    @patch('os.path.join')
    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet', side_effect=Exception("Parquet save failed"))
    def test_save_player_profile_parquet_error(self, mock_save_parquet, mock_save_json, mock_os_path_join, mock_makedirs, mock_json_normalize, mock_logger):
        """Test when an error occurs during Parquet saving in save_player_profile."""
        mock_json_normalize.return_value = pd.DataFrame({'col1': [1], 'col2': [2]})
        mock_os_path_join.side_effect = ["data/processed/player_test_profile.parquet", "data/raw/player_test_profile.json"]

        profile = {"name": "Test Player", "level": 10}
        player_id = "test_player"

        save_player_profile(profile, player_id)

        mock_logger.info.assert_any_call(f"Saving profile for player ID: {player_id}.")
        mock_json_normalize.assert_called_once_with(profile)
        mock_makedirs.assert_any_call("data/processed", exist_ok=True)
        mock_os_path_join.assert_any_call("data/processed", f"player_{player_id}_profile.parquet")
        mock_logger.info.assert_any_call(f"Saving {player_id} profile to Parquet file.")
        mock_save_parquet.assert_called_once()
        # We don't need to assert the arguments of mock_save_parquet here as we're focusing on the error

        mock_makedirs.assert_any_call("data/raw", exist_ok=True)
        mock_os_path_join.assert_any_call("data/raw", f"player_{player_id}_profile.json")
        mock_logger.info.assert_any_call(f"Saving {player_id} profile to json file.")
        mock_save_json.assert_called_once_with(profile, "data/raw/player_test_profile.json")
        mock_logger.error.assert_any_call(f"Error saving parquet profile for player {player_id}: Parquet save failed")

    @patch('src.processing.data_saver.logger')
    @patch('pandas.json_normalize')
    @patch('os.makedirs')
    @patch('os.path.join')
    @patch('src.processing.data_saver._save_json', side_effect=Exception("JSON save failed"))
    @patch('src.processing.data_saver._save_parquet')
    def test_save_player_profile_json_error(self, mock_save_parquet, mock_save_json, mock_os_path_join, mock_makedirs, mock_json_normalize, mock_logger):
        """Test when an error occurs during JSON saving in save_player_profile."""
        expected_df = pd.DataFrame({'col1': [1], 'col2': [2]})
        mock_json_normalize.return_value = expected_df
        mock_os_path_join.side_effect = ["data/processed/player_test_profile.parquet", "data/raw/player_test_profile.json"]

        profile = {"name": "Test Player", "level": 10}
        player_id = "test_player"

        save_player_profile(profile, player_id)

        mock_logger.info.assert_any_call(f"Saving profile for player ID: {player_id}.")
        mock_json_normalize.assert_called_once_with(profile)
        mock_makedirs.assert_any_call("data/processed", exist_ok=True)
        mock_os_path_join.assert_any_call("data/processed", f"player_{player_id}_profile.parquet")
        mock_logger.info.assert_any_call(f"Saving {player_id} profile to Parquet file.")
        mock_save_parquet.assert_called_once()
        call_args_parquet = mock_save_parquet.call_args[0]
        self.assertTrue(call_args_parquet[0].equals(expected_df))
        self.assertEqual(call_args_parquet[1], "data/processed/player_test_profile.parquet")

        mock_makedirs.assert_any_call("data/raw", exist_ok=True)
        mock_os_path_join.assert_any_call("data/raw", f"player_{player_id}_profile.json")
        mock_logger.info.assert_any_call(f"Saving {player_id} profile to json file.")
        mock_save_json.assert_called_once_with(profile, "data/raw/player_test_profile.json")
        mock_logger.error.assert_any_call(f"Error saving json profile for player {player_id}: JSON save failed")

    @patch('src.processing.data_saver.logger')
    @patch('pandas.json_normalize')
    @patch('os.makedirs', side_effect=[OSError("Cannot create directory: data/processed"), None])
    @patch('os.path.join', side_effect=lambda *args: "data/processed/player_test_profile.parquet" if "processed" in args[0] else "data/raw/player_test_profile.json")
    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    def test_save_player_profile_parquet_makedirs_error(self, mock_save_parquet, mock_save_json, mock_os_path_join, mock_makedirs, mock_json_normalize, mock_logger):
        """Test when os.makedirs raises an error for the Parquet directory."""
        mock_json_normalize.return_value = pd.DataFrame({'col1': [1], 'col2': [2]})

        profile = {"name": "Test Player", "level": 10}
        player_id = "test_player"

        save_player_profile(profile, player_id)

        mock_logger.info.assert_any_call(f"Saving profile for player ID: {player_id}.")
        mock_json_normalize.assert_called_once_with(profile)
        mock_makedirs.assert_any_call("data/processed", exist_ok=True)
        mock_makedirs.assert_any_call("data/raw", exist_ok=True)
        mock_save_parquet.assert_not_called()
        mock_save_json.assert_called_once_with(profile, "data/raw/player_test_profile.json")
        mock_logger.error.assert_any_call(f"Error saving parquet profile for player {player_id}: Cannot create directory: data/processed")

    pass

class TestExtractResultsData(unittest.TestCase):

    def test_extract_results_data_empty(self):
        """Test with an empty results dictionary."""
        results = {}
        extracted_data = _extract_results_data(results)
        self.assertEqual(extracted_data, [])

    def test_extract_results_data_no_draws(self):
        """Test with results containing events but no draws."""
        results = {
            "events": [
                {
                    "name": "Event 1",
                    "id": 101
                },
                {
                    "name": "Event 2",
                    "id": 102
                }
            ]
        }
        extracted_data = _extract_results_data(results)
        self.assertEqual(extracted_data, [])

    def test_extract_results_data_no_results_in_draws(self):
        """Test with results containing events and draws, but no results within draws."""
        results = {
            "events": [
                {
                    "name": "Event A",
                    "id": 201,
                    "draws": [
                        {
                            "name": "Draw X",
                            "id": 301
                        },
                        {
                            "name": "Draw Y",
                            "id": 302
                        }
                    ]
                },
                {
                    "name": "Event B",
                    "id": 202,
                    "draws": []  # Empty draws list
                }
            ]
        }
        extracted_data = _extract_results_data(results)
        self.assertEqual(extracted_data, [])

    def test_extract_results_data_simple_results(self):
        """Test with results directly within a draw."""
        results = {
            "events": [
                {
                    "name": "Simple Event",
                    "id": 401,
                    "draws": [
                        {
                            "name": "Main Draw",
                            "id": 501,
                            "results": [
                                {"players": {"player1": "Alice", "player2": "Bob"}, "score": {"score": "6-3, 7-5"}},
                                {"players": {"player1": "Charlie", "player2": "David"}, "score": {"score": "7-6(2), 6-4"}}
                            ]
                        }
                    ]
                }
            ]
        }
        expected_data = [
            {
                "event_id": 401,
                "event_name": "Simple Event",
                "draw_id": 501,
                "draw_name": "Main Draw",
                "result_id": None,
                "date": None,
                "players": json.dumps({"player1": "Alice", "player2": "Bob"}),
                "score": json.dumps({"score": "6-3, 7-5"}),
                "teamType": None,
                "sportTypeId": None,
                "sourceType": None,
                "completionType": None,
                "outcome": None,
                "finalized": None,
            },
            {
                "event_id": 401,
                "event_name": "Simple Event",
                "draw_id": 501,
                "draw_name": "Main Draw",
                "result_id": None,
                "date": None,
                "players": json.dumps({"player1": "Charlie", "player2": "David"}),
                "score": json.dumps({"score": "7-6(2), 6-4"}),
                "teamType": None,
                "sportTypeId": None,
                "sourceType": None,
                "completionType": None,
                "outcome": None,
                "finalized": None,
            },
        ]
        extracted_data = _extract_results_data(results)
        self.assertEqual(extracted_data, expected_data)

    def test_extract_results_data_missing_keys(self):
        """Test with missing keys in events and draws."""
        results = {
            "events": [
                {
                    "id": 801,  # Missing "name"
                    "draws": [
                        {
                            "name": "Draw Without ID",  # Missing "id"
                            "results": [
                                {"players": {"player1": "Ivy", "player2": "Jack"}, "score": {"score": "6-0, 6-0"}}
                            ]
                        }
                    ]
                },
                {
                    "name": "Event Without Draws",
                    "id": 802,
                }
            ]
        }
        expected_data = [
            {
                "event_id": 801,
                "event_name": None,
                "draw_id": None,
                "draw_name": "Draw Without ID",
                "result_id": None,
                "date": None,
                "players": json.dumps({"player1": "Ivy", "player2": "Jack"}),
                "score": json.dumps({"score": "6-0, 6-0"}),
                "teamType": None,
                "sportTypeId": None,
                "sourceType": None,
                "completionType": None,
                "outcome": None,
                "finalized": None,
            },
        ]
        extracted_data = _extract_results_data(results)
        self.assertEqual(extracted_data, expected_data)
    pass

class TestValidateResultsData(unittest.TestCase):

    def test_validate_results_data_valid_input(self):
        """Test with a valid DataFrame containing a 'date' column."""
        valid_data = [
            {
                "event_id": 1,
                "event_name": "Tournament A",
                "draw_id": 101,
                "draw_name": "Main Draw",
                "result_id": 1001,
                "date": "2025-03-31",
                "players": json.dumps({"player1": "Player One", "player2": "Player Two"}),
                "score": json.dumps({"score": "6-4, 6-3"}),
                "teamType": "Individual",
                "sportTypeId": 1,
                "sourceType": "API",
                "completionType": "Finished",
                "outcome": "Player One Won",
                "finalized": True,
            },
            {
                "event_id": 2,
                "event_name": "Tournament B",
                "draw_id": 201,
                "draw_name": "Qualifying",
                "result_id": 2001,
                "date": "2025-03-30",
                "players": json.dumps({"player1": "Player Three", "player2": "Player Four"}),
                "score": json.dumps({"score": "7-6, 3-6, 7-5"}),
                "teamType": "Individual",
                "sportTypeId": 1,
                "sourceType": "API",
                "completionType": "Finished",
                "outcome": "Player Four Won",
                "finalized": True,
            },
        ]
        df = pd.DataFrame(valid_data)
        is_valid = validate_results_data(df)
        self.assertTrue(is_valid)

    @patch('src.processing.data_saver.logger')
    def test_validate_results_data_empty_dataframe(self, mock_logger):
        """Test with an empty DataFrame."""
        empty_df = pd.DataFrame()
        is_valid = validate_results_data(empty_df)
        self.assertFalse(is_valid)
        mock_logger.warning.assert_called_once_with("Results DataFrame is empty.")

    @patch('src.processing.data_saver.logger')
    def test_validate_results_data_missing_date_column(self, mock_logger):
        """Test with a DataFrame missing the 'date' column."""
        invalid_data = [
            {
                "event_id": 1,
                "event_name": "Tournament A",
                "draw_id": 101,
                "draw_name": "Main Draw",
                "result_id": 1001,
                "players": json.dumps({"player1": "Player One", "player2": "Player Two"}),
                "score": json.dumps({"score": "6-4, 6-3"}),
                "teamType": "Individual",
                "sportTypeId": 1,
                "sourceType": "API",
                "completionType": "Finished",
                "outcome": "Player One Won",
                "finalized": True,
            },
            # Note: No "date" column
        ]
        df = pd.DataFrame(invalid_data)
        is_valid = validate_results_data(df)
        self.assertFalse(is_valid)
        mock_logger.error.assert_called_once_with("Results DataFrame missing 'date' column.")

    pass

class TestSavePlayerResults(unittest.TestCase):

    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    @patch('src.processing.data_saver.validate_results_data')
    @patch('src.processing.data_saver._extract_results_data')
    @patch('src.processing.data_saver.logger')
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_save_player_results_success(self, mock_os_path_join, mock_makedirs, mock_logger, mock_extract, mock_validate, mock_save_parquet, mock_save_json):
        """Test a successful run of save_player_results."""
        player_id = "test_player"
        results = {"some": "results data"}
        extracted_data = [{"match": 1}, {"match": 2}]
        mock_extract.return_value = extracted_data
        mock_validate.return_value = True
        mock_os_path_join.side_effect = [
            f"data/processed/player_{player_id}_results.parquet",
            f"data/raw/player_{player_id}_results.json"
        ]

        save_player_results(results, player_id)

        mock_logger.info.assert_any_call(f"Saving results for player ID: {player_id}.")
        mock_extract.assert_called_once_with(results)
        pd.testing.assert_frame_equal(pd.DataFrame(extracted_data), pd.DataFrame(mock_save_parquet.call_args[0][0]))
        pd.testing.assert_frame_equal(pd.DataFrame(extracted_data), mock_validate.call_args[0][0])
        mock_makedirs.assert_any_call("data/processed", exist_ok=True)
        mock_logger.info.assert_any_call(f"Saving {player_id} results DataFrame with {len(extracted_data)} rows to Parquet file.")
        mock_save_parquet.assert_called_once()
        mock_makedirs.assert_any_call("data/raw", exist_ok=True)
        mock_logger.info.assert_any_call(f"Saving {player_id} results to json file.")
        mock_save_json.assert_called_once_with(results, f"data/raw/player_{player_id}_results.json")

    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    @patch('src.processing.data_saver.validate_results_data')
    @patch('src.processing.data_saver._extract_results_data')
    @patch('src.processing.data_saver.logger')
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_save_player_results_empty_extracted_data(self, mock_os_path_join, mock_makedirs, mock_logger, mock_extract, mock_validate, mock_save_parquet, mock_save_json):
        """Test when _extract_results_data returns no data."""
        player_id = "test_player"
        results = {"some": "results data"}
        mock_extract.return_value = []

        def validate_side_effect(df):
            if df.empty:
                mock_logger.warning("Results DataFrame is empty.")
                return False
            return True

        mock_validate.side_effect = validate_side_effect

        save_player_results(results, player_id)

        mock_logger.info.assert_any_call(f"Saving results for player ID: {player_id}.")
        mock_extract.assert_called_once_with(results)
        mock_validate.assert_called_once()
        pd.testing.assert_frame_equal(pd.DataFrame([]), mock_validate.call_args[0][0])
        mock_logger.warning.assert_called_once_with("Results DataFrame is empty.")
        mock_save_parquet.assert_not_called()
        mock_makedirs.assert_not_called()
        expected_parquet_log_message = f"Saving {player_id} results DataFrame with 0 rows to Parquet file."
        self.assertNotIn(unittest.mock.call(expected_parquet_log_message), mock_logger.info.call_args_list)
        mock_save_json.assert_not_called()
        expected_json_log_message = f"Saving {player_id} results to json file."
        self.assertNotIn(unittest.mock.call(expected_json_log_message), mock_logger.info.call_args_list)

    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    @patch('src.processing.data_saver.validate_results_data')
    @patch('src.processing.data_saver._extract_results_data')
    @patch('src.processing.data_saver.logger')
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_save_player_results_validation_fails_missing_date(self, mock_os_path_join, mock_makedirs, mock_logger, mock_extract, mock_validate, mock_save_parquet, mock_save_json):
        """Test when validate_results_data returns False due to a missing 'date' column."""
        player_id = "test_player"
        results = {"some": "results data"}
        extracted_data = [{"match": 1}, {"match": 2}]
        mock_extract.return_value = extracted_data
        invalid_df = pd.DataFrame(extracted_data)
        mock_validate.return_value = False

        save_player_results(results, player_id)

        mock_logger.info.assert_any_call(f"Saving results for player ID: {player_id}.")
        mock_extract.assert_called_once_with(results)
        mock_validate.assert_called_once()
        pd.testing.assert_frame_equal(invalid_df, mock_validate.call_args[0][0])
        mock_save_parquet.assert_not_called()
        mock_makedirs.assert_not_called()
        mock_save_json.assert_not_called()
        expected_json_log_message = f"Saving {player_id} results to json file."
        self.assertNotIn(unittest.mock.call(expected_json_log_message), mock_logger.info.call_args_list)

    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    @patch('src.processing.data_saver.validate_results_data')
    @patch('src.processing.data_saver._extract_results_data')
    @patch('src.processing.data_saver.logger')
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_save_player_results_parquet_error(self, mock_os_path_join, mock_makedirs, mock_logger, mock_extract, mock_validate, mock_save_parquet, mock_save_json):
        """Test when _save_parquet raises an exception."""
        player_id = "test_player"
        results = {"some": "results data"}
        extracted_data = [{"match": 1}, {"match": 2}]
        mock_extract.return_value = extracted_data
        mock_validate.return_value = True
        mock_os_path_join.side_effect = [
            f"data/processed/player_{player_id}_results.parquet",
            f"data/raw/player_{player_id}_results.json"
        ]
        parquet_error_message = "Error saving Parquet file"
        mock_save_parquet.side_effect = Exception(parquet_error_message)

        save_player_results(results, player_id)

        mock_logger.info.assert_any_call(f"Saving results for player ID: {player_id}.")
        mock_extract.assert_called_once_with(results)
        mock_validate.assert_called_once()
        pd.testing.assert_frame_equal(pd.DataFrame(extracted_data), mock_validate.call_args[0][0])
        mock_makedirs.assert_any_call("data/processed", exist_ok=True)
        mock_logger.error.assert_called_once_with(f"Error saving results for player {player_id}: {parquet_error_message}")
        mock_save_json.assert_called_once_with(results, f"data/raw/player_{player_id}_results.json")

    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    @patch('src.processing.data_saver.validate_results_data')
    @patch('src.processing.data_saver._extract_results_data')
    @patch('src.processing.data_saver.logger')
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_save_player_results_json_error(self, mock_os_path_join, mock_makedirs, mock_logger, mock_extract, mock_validate, mock_save_parquet, mock_save_json):
        """Test when _save_json raises an exception."""
        player_id = "test_player"
        results = {"some": "results data"}
        extracted_data = [{"match": 1}, {"match": 2}]
        mock_extract.return_value = extracted_data
        mock_validate.return_value = True
        mock_os_path_join.side_effect = [
            f"data/processed/player_{player_id}_results.parquet",
            f"data/raw/player_{player_id}_results.json"
        ]
        json_error_message = "Error saving JSON file"
        mock_save_json.side_effect = Exception(json_error_message)

        save_player_results(results, player_id)

        mock_logger.info.assert_any_call(f"Saving results for player ID: {player_id}.")
        mock_extract.assert_called_once_with(results)
        mock_validate.assert_called_once()
        pd.testing.assert_frame_equal(pd.DataFrame(extracted_data), mock_validate.call_args[0][0])
        mock_makedirs.assert_any_call("data/raw", exist_ok=True)
        mock_logger.error.assert_called_once_with(f"Error saving json results for player {player_id}: {json_error_message}")
        mock_save_parquet.assert_called_once()

class TestSavePlayerProfile(unittest.TestCase):

    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    @patch('src.processing.data_saver.logger')
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_save_player_profile_success(self, mock_os_path_join, mock_makedirs, mock_logger, mock_save_parquet, mock_save_json):
        """Test a successful run of save_player_profile."""
        player_id = "test_player"
        profile = {"name": "Test Player", "rank": 10}
        mock_os_path_join.side_effect = [
            f"data/processed/player_{player_id}_profile.parquet",
            f"data/raw/player_{player_id}_profile.json"
        ]

        save_player_profile(profile, player_id)

        mock_logger.info.assert_any_call(f"Saving profile for player ID: {player_id}.")
        pd.testing.assert_frame_equal(pd.DataFrame([profile]), mock_save_parquet.call_args[0][0])
        mock_makedirs.assert_any_call("data/processed", exist_ok=True)
        mock_logger.info.assert_any_call(f"Saving {player_id} profile to Parquet file.")
        mock_save_parquet.assert_called_once()
        mock_makedirs.assert_any_call("data/raw", exist_ok=True)
        mock_logger.info.assert_any_call(f"Saving {player_id} profile to json file.")
        mock_save_json.assert_called_once_with(profile, f"data/raw/player_{player_id}_profile.json")

    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    @patch('src.processing.data_saver.logger')
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_save_player_profile_parquet_error(self, mock_os_path_join, mock_makedirs, mock_logger, mock_save_parquet, mock_save_json):
        """Test when _save_parquet raises an exception."""
        player_id = "error_player"
        profile = {"name": "Error Player", "rank": 99}
        mock_os_path_join.side_effect = [
            f"data/processed/player_{player_id}_profile.parquet",
            f"data/raw/player_{player_id}_profile.json"
        ]
        parquet_error_message = "Error saving profile to Parquet"
        mock_save_parquet.side_effect = Exception(parquet_error_message)

        save_player_profile(profile, player_id)

        mock_logger.info.assert_any_call(f"Saving profile for player ID: {player_id}.")
        pd.testing.assert_frame_equal(pd.DataFrame([profile]), mock_save_parquet.call_args[0][0])
        mock_makedirs.assert_any_call("data/processed", exist_ok=True)
        mock_logger.error.assert_called_once_with(f"Error saving parquet profile for player {player_id}: {parquet_error_message}")
        mock_save_json.assert_called_once_with(profile, f"data/raw/player_{player_id}_profile.json")

    @patch('src.processing.data_saver._save_json')
    @patch('src.processing.data_saver._save_parquet')
    @patch('src.processing.data_saver.logger')
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_save_player_profile_json_error(self, mock_os_path_join, mock_makedirs, mock_logger, mock_save_parquet, mock_save_json):
        """Test when _save_json raises an exception."""
        player_id = "json_error_player"
        profile = {"name": "JSON Error Player", "country": "Unknown"}
        mock_os_path_join.side_effect = [
            f"data/processed/player_{player_id}_profile.parquet",
            f"data/raw/player_{player_id}_profile.json"
        ]
        json_error_message = "Error saving profile to JSON"
        mock_save_json.side_effect = Exception(json_error_message)

        save_player_profile(profile, player_id)

        mock_logger.info.assert_any_call(f"Saving profile for player ID: {player_id}.")
        pd.testing.assert_frame_equal(pd.DataFrame([profile]), mock_save_parquet.call_args[0][0])
        mock_makedirs.assert_any_call("data/raw", exist_ok=True)
        mock_logger.error.assert_called_once_with(f"Error saving json profile for player {player_id}: {json_error_message}")
        mock_save_parquet.assert_called_once()

if __name__ == '__main__':
    unittest.main()
