import unittest
from unittest.mock import patch, mock_open, MagicMock
import pandas as pd
import json
import os
import logging
import sqlite3
import tempfile
from src.processing.data_saver import _save_parquet, _save_json, save_player_profile, _extract_results_data, validate_results_data, save_player_results, save_player_profile
from src.team.team import Team
from src.team.team_manager import TeamManager
from src.player.player_data_loader import load_player_profile_from_db, load_multiple_player_profiles_from_db

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
    @patch('os.makedirs', side_effect=[OSError("Cannot create directory: data/processed"), None, None])
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
    pass

class TestTeamManager(unittest.TestCase):

    def setUp(self):
        """Set up for test methods using temporary file-based databases."""
        # Setup for teams database
        self.teams_temp_db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.teams_db_file = self.teams_temp_db_file.name
        self.teams_temp_db_file.close()
        self.team_manager = TeamManager(db_file=self.teams_db_file)

        # Setup for players test database
        self.players_temp_db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.players_db_file = self.players_temp_db_file.name
        self.players_temp_db_file.close()
        self._create_test_players_table()
        self._populate_test_players_table()

    def tearDown(self):
        """Clean up by deleting the temporary database files."""
        os.remove(self.teams_db_file)
        os.remove(self.players_db_file)

    def test_create_team(self):
        """Test creating a new team and verifying it's in the database."""
        team_number = "12345"
        year = 2025
        league_type = "Adult 40 & Over"
        section = "USTA/SOUTHERN"
        district = "GEORGIA"
        area = "GA - COLUMBUS - CORTA"
        season = "2025 CORTA USTA Adult 40 & Over - Spring"
        flight = "Men 3.0"
        name = "Test Team-CC"

        self.team_manager = TeamManager(db_file=self.team_manager.db_file)

        # Inspect the schema immediately after TeamManager initialization
        conn = sqlite3.connect(self.team_manager.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT type, name, sql FROM sqlite_schema")
        schema_info = cursor.fetchall()
        conn.close()

        team = self.team_manager.create_team(team_number, year, league_type, section, district, area, season, flight, name)

        self.assertIsInstance(team, Team)
        self.assertEqual(team.team_number, team_number)
        self.assertEqual(team.name, name)
        self.assertEqual(team.players, [])

        # Verify the team is in the database
        conn = sqlite3.connect(self.team_manager.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM teams WHERE team_number=?", (team_number,))
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], team_number)
        self.assertEqual(row[8], name)

    def _create_test_players_table(self):
        """Creates the players table in the test database."""
        conn = sqlite3.connect(self.players_db_file)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id TEXT PRIMARY KEY,
                firstName TEXT,
                lastName TEXT,
                gender TEXT,
                birthDate TEXT,
                ageRange TEXT,
                displayName TEXT,
                myUtrSingles REAL,
                myUtrDoubles REAL,
                descriptionShort TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _populate_test_players_table(self):
        """Populates the test players table with some dummy data."""
        conn = sqlite3.connect(self.players_db_file)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO players (id, firstName) VALUES (?, ?)
        """, ("player_alpha", "Alpha"))
        cursor.execute("""
            INSERT OR IGNORE INTO players (id, firstName) VALUES (?, ?)
        """, ("player_beta", "Beta"))
        conn.commit()
        conn.close()

    def test_get_team(self):
        """Test retrieving a team from the database by team number."""
        team_number = "54321"
        year = 2024
        league_type = "Mixed 40 & Over"
        section = "USTA/SOUTHERN"
        district = "GEORGIA"
        area = "GA - COLUMBUS - CORTA"
        season = "2024 CORTA USTA Mixed 40 & Over - Fall"
        flight = "Mixed 7.0"
        name = "Test Mixed Team-GI"

        # Create and add the team to the database
        team = self.team_manager.create_team(team_number, year, league_type, section, district, area, season, flight, name)

        # Retrieve the team using get_team
        retrieved_team = self.team_manager.get_team(team_number)

        # Assert that the retrieved team is not None and has the correct attributes
        self.assertIsNotNone(retrieved_team)
        self.assertIsInstance(retrieved_team, Team)
        self.assertEqual(retrieved_team.team_number, team_number)
        self.assertEqual(retrieved_team.year, year)
        self.assertEqual(retrieved_team.league_type, league_type)
        self.assertEqual(retrieved_team.section, section)
        self.assertEqual(retrieved_team.district, district)
        self.assertEqual(retrieved_team.area, area)
        self.assertEqual(retrieved_team.season, season)
        self.assertEqual(retrieved_team.flight, flight)
        self.assertEqual(retrieved_team.name, name)
        self.assertEqual(retrieved_team.players, [])

    def test_add_player_to_team(self):
        """Test adding players to a team and verifying the roster."""
        team_number = "98765"
        year = 2023
        league_type = "Adult 55 & Over"
        section = "Midwest"
        district = "OH"
        area = "Cincinnati"
        season = "Summer"
        flight = "Men's 4.0"
        name = "Test Senior Team"

        player_id_1 = "player_a"
        player_id_2 = "player_b"

        # Create a team
        team = self.team_manager.create_team(team_number, year, league_type, section, district, area, season, flight, name)

        # Add players to the team
        self.team_manager.add_player_to_team(team_number, player_id_1)
        self.team_manager.add_player_to_team(team_number, player_id_2)

        # Retrieve the team
        retrieved_team = self.team_manager.get_team(team_number)

        # Assert that the players are in the retrieved team's roster
        self.assertIsNotNone(retrieved_team)
        self.assertIn(player_id_1, retrieved_team.players)
        self.assertIn(player_id_2, retrieved_team.players)
        self.assertEqual(len(retrieved_team.players), 2)

        # Verify the team_players table
        conn = sqlite3.connect(self.team_manager.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT player_id FROM team_players WHERE team_number=?", (team_number,))
        player_links = [row[0] for row in cursor.fetchall()]
        conn.close()

        self.assertIn(player_id_1, player_links)
        self.assertIn(player_id_2, player_links)
        self.assertEqual(len(player_links), 2)

    def test_remove_player_from_team(self):
        """Test removing a player from a team and verifying the roster."""
        team_number = "11223"
        year = 2026
        league_type = "Adult 18 & Over"
        section = "Texas"
        district = "NTX"
        area = "Dallas"
        season = "Fall"
        flight = "Women's 3.0"
        name = "Test Removal Team"

        player_id_1 = "player_x"
        player_id_2 = "player_y"

        # Create a team and add players
        self.team_manager.create_team(team_number, year, league_type, section, district, area, season, flight, name)
        self.team_manager.add_player_to_team(team_number, player_id_1)
        self.team_manager.add_player_to_team(team_number, player_id_2)

        # Remove one of the players
        self.team_manager.remove_player_from_team(team_number, player_id_1)

        # Retrieve the team
        retrieved_team = self.team_manager.get_team(team_number)

        # Assert that the removed player is no longer in the roster, but the other is
        self.assertIsNotNone(retrieved_team)
        self.assertNotIn(player_id_1, retrieved_team.players)
        self.assertIn(player_id_2, retrieved_team.players)
        self.assertEqual(len(retrieved_team.players), 1)

        # Verify the team_players table
        conn = sqlite3.connect(self.team_manager.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT player_id FROM team_players WHERE team_number=?", (team_number,))
        player_links = [row[0] for row in cursor.fetchall()]
        conn.close()

        self.assertNotIn(player_id_1, player_links)
        self.assertIn(player_id_2, player_links)
        self.assertEqual(len(player_links), 1)

    def test_duplicate_team_number(self):
        """Test handling of attempting to create a team with a duplicate team number."""
        team_number = "DUPLICATE1"
        year_1 = 2025
        league_type_1 = "Adult 18 & Over"
        name_1 = "Original Team"

        year_2 = 2026
        league_type_2 = "Mixed 40 & Over"
        name_2 = "Duplicate Team"

        # Create and add the first team
        team_1 = self.team_manager.create_team(team_number, year_1, league_type_1, "Section A", "District 1", "Area X", "Spring", "Flight 1", name_1)

        # Attempt to create a second team with the same team_number
        team_2 = self.team_manager.create_team(team_number, year_2, league_type_2, "Section B", "District 2", "Area Y", "Fall", "Flight 2", name_2)

        # The second create_team call should not raise an exception
        self.assertIsInstance(team_1, Team)
        self.assertIsInstance(team_2, Team) # It will still return a Team object

        # Verify that only one team with this number exists in the database
        conn = sqlite3.connect(self.team_manager.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM teams WHERE team_number=?", (team_number,))
        count = cursor.fetchone()[0]
        conn.close()

        self.assertEqual(count, 1)

        # Optionally, you could also verify that the attributes of the existing team are still from the first creation (team_1)
        retrieved_team = self.team_manager.get_team(team_number)
        self.assertIsNotNone(retrieved_team)
        self.assertEqual(retrieved_team.name, name_1)
        self.assertEqual(retrieved_team.year, year_1)
        self.assertEqual(retrieved_team.league_type, league_type_1)

    def test_add_duplicate_player_to_team(self):
        """Test handling of attempting to add the same player to a team multiple times."""
        team_number = "DUP_PLAYER_TEAM"
        year = 2024
        league_type = "Adult 40 & Over"
        name = "Duplicate Player Test Team"
        player_id = "repeated_player"

        # Create a team
        self.team_manager.create_team(team_number, year, league_type, "Section C", "District 3", "Area Z", "Summer", "Flight 3", name)

        # Add the player to the team multiple times
        self.team_manager.add_player_to_team(team_number, player_id)
        self.team_manager.add_player_to_team(team_number, player_id)
        self.team_manager.add_player_to_team(team_number, player_id)

        # Retrieve the team
        retrieved_team = self.team_manager.get_team(team_number)

        # Assert that the player is in the roster only once
        self.assertIsNotNone(retrieved_team)
        self.assertIn(player_id, retrieved_team.players)
        self.assertEqual(len(retrieved_team.players), 1)

        # Verify the team_players table contains only one entry for this player and team
        conn = sqlite3.connect(self.team_manager.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM team_players WHERE team_number=? AND player_id=?", (team_number, player_id))
        count = cursor.fetchone()[0]
        conn.close()

        self.assertEqual(count, 1)

    def test_populate_roster(self):
        """Test populating a team's roster using player IDs from the test database."""
        team_number = "ROSTER_TEAM"
        year = 2025
        league_type = "Adult Mixed"
        name = "Roster Test Team"

        player_id_1 = "player_alpha"
        player_id_2 = "player_beta"
        player_id_3 = "player_gamma"  # This player should not exist in the test db

        # Create a team and add it to the database
        self.team_manager.create_team(team_number, year, league_type, "S", "D", "A", "Spr", "F", name)

        # Ensure some dummy player profiles exist in the database
        conn = sqlite3.connect(self.players_db_file)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO players (id, firstName) VALUES (?, ?)
        """, (player_id_1, "Alpha"))
        cursor.execute("""
            INSERT OR IGNORE INTO players (id, firstName) VALUES (?, ?)
        """, (player_id_2, "Beta"))
        conn.commit()
        conn.close()

        player_ids_to_add = [player_id_1, player_id_2, player_id_3]

        # We need to temporarily patch the player data loader to use our test database file
        with patch('src.player.player_data_loader.PLAYERS_DB_FILE', self.players_db_file):
            # Retrieve the team from the database
            retrieved_team = self.team_manager.get_team(team_number)
            self.assertIsNotNone(retrieved_team)

            # Populate the roster of the retrieved team
            self.team_manager.populate_roster(retrieved_team, player_ids_to_add)

            # Assert that the retrieved team's roster contains the players found in the test database
            self.assertIn(player_id_1, retrieved_team.players)
            self.assertIn(player_id_2, retrieved_team.players)
            self.assertNotIn(player_id_3, retrieved_team.players)
            self.assertEqual(len(retrieved_team.players), 2)

if __name__ == '__main__':
    unittest.main()
