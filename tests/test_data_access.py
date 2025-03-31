import unittest
from unittest.mock import patch, mock_open
import pandas as pd
import json
import os
from src.data_access import load_player_results, load_player_stats, load_player_profile

class TestDataAccessLoadPlayerResults(unittest.TestCase):

    @patch('pandas.read_parquet')
    @patch('os.path.join')
    def test_load_player_results_success(self, mock_os_path_join, mock_read_parquet):
        """Test successful loading of player results."""
        player_id = "test_player"
        expected_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        mock_read_parquet.return_value = expected_df
        mock_os_path_join.return_value = f"data/processed/player_{player_id}_results.parquet"

        actual_df = load_player_results(player_id)

        self.assertTrue(expected_df.equals(actual_df))
        mock_os_path_join.assert_called_once_with("data/processed", f"player_{player_id}_results.parquet")
        mock_read_parquet.assert_called_once_with(f"data/processed/player_{player_id}_results.parquet")

    @patch('pandas.read_parquet')
    @patch('os.path.join')
    def test_load_player_results_file_not_found(self, mock_os_path_join, mock_read_parquet):
        """Test when the player results file is not found."""
        player_id = "missing_player"
        mock_read_parquet.side_effect = FileNotFoundError()
        mock_os_path_join.return_value = f"data/processed/player_{player_id}_results.parquet"

        result = load_player_results(player_id)

        self.assertIsNone(result)
        mock_os_path_join.assert_called_once_with("data/processed", f"player_{player_id}_results.parquet")
        mock_read_parquet.assert_called_once_with(f"data/processed/player_{player_id}_results.parquet")
        self.assertLogs('src.data_access', level='ERROR')

    @patch('pandas.read_parquet')
    @patch('os.path.join')
    def test_load_player_results_parquet_error(self, mock_os_path_join, mock_read_parquet):
        """Test when an error occurs during Parquet reading."""
        player_id = "error_player"
        mock_read_parquet.side_effect = ValueError("Parquet file is corrupted")
        mock_os_path_join.return_value = f"data/processed/player_{player_id}_results.parquet"

        result = load_player_results(player_id)

        self.assertIsNone(result)
        mock_os_path_join.assert_called_once_with("data/processed", f"player_{player_id}_results.parquet")
        mock_read_parquet.assert_called_once_with(f"data/processed/player_{player_id}_results.parquet")
        self.assertLogs('src.data_access', level='ERROR')

    pass

class TestDataAccessLoadPlayerStats(unittest.TestCase):

    @patch('os.path.join')
    @patch('builtins.open', new_callable=mock_open, read_data='{"stats": [{"type": "singles", "wins": 10}]}')
    @patch('json.load')
    def test_load_player_stats_success(self, mock_json_load, mock_file, mock_os_path_join):
        """Test successful loading of player stats."""
        player_id = "test_player"
        match_type = "singles"
        expected_stats = {"stats": [{"type": "singles", "wins": 10}]}
        mock_json_load.return_value = expected_stats
        mock_os_path_join.return_value = f"data/raw/player_{player_id}_{match_type}_stats.json"

        actual_stats = load_player_stats(player_id, match_type)

        self.assertEqual(actual_stats, expected_stats)
        mock_os_path_join.assert_called_once_with("data/raw", f"player_{player_id}_{match_type}_stats.json")
        mock_file.assert_called_once_with(f"data/raw/player_{player_id}_{match_type}_stats.json", "r")
        mock_json_load.assert_called_once_with(mock_file.return_value)

    @patch('os.path.join')
    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_load_player_stats_file_not_found(self, mock_file, mock_os_path_join):
        """Test when the player stats file is not found."""
        player_id = "missing_player"
        match_type = "doubles"
        mock_os_path_join.return_value = f"data/raw/player_{player_id}_{match_type}_stats.json"

        result = load_player_stats(player_id, match_type)

        self.assertIsNone(result)
        mock_os_path_join.assert_called_once_with("data/raw", f"player_{player_id}_{match_type}_stats.json")
        mock_file.assert_called_once_with(f"data/raw/player_{player_id}_{match_type}_stats.json", "r")
        self.assertLogs('src.data_access', level='ERROR')

    @patch('os.path.join')
    @patch('builtins.open', new_callable=mock_open, read_data='{"stats": "invalid json"}')
    @patch('json.load', side_effect=json.JSONDecodeError("Expecting value", '{"stats": "invalid json"}', 10))
    def test_load_player_stats_invalid_json(self, mock_json_load, mock_file, mock_os_path_join):
        """Test when the player stats file contains invalid JSON."""
        player_id = "bad_json_player"
        match_type = "singles"
        mock_os_path_join.return_value = f"data/raw/player_{player_id}_{match_type}_stats.json"

        result = load_player_stats(player_id, match_type)

        self.assertIsNone(result)
        mock_os_path_join.assert_called_once_with("data/raw", f"player_{player_id}_{match_type}_stats.json")
        mock_file.assert_called_once_with(f"data/raw/player_{player_id}_{match_type}_stats.json", "r")
        mock_json_load.assert_called_once_with(mock_file.return_value)
        self.assertLogs('src.data_access', level='ERROR')

    @patch('os.path.join')
    @patch('builtins.open', side_effect=IOError("Error reading file"))
    def test_load_player_stats_io_error(self, mock_file, mock_os_path_join):
        """Test when an IOError occurs during file reading."""
        player_id = "io_error_player"
        match_type = "doubles"
        mock_os_path_join.return_value = f"data/raw/player_{player_id}_{match_type}_stats.json"

        result = load_player_stats(player_id, match_type)

        self.assertIsNone(result)
        mock_os_path_join.assert_called_once_with("data/raw", f"player_{player_id}_{match_type}_stats.json")
        mock_file.assert_called_once_with(f"data/raw/player_{player_id}_{match_type}_stats.json", "r")
        self.assertLogs('src.data_access', level='ERROR')

    pass

class TestDataAccessLoadPlayerProfile(unittest.TestCase):

    @patch('pandas.read_parquet')
    @patch('os.path.join')
    def test_load_player_profile_success(self, mock_os_path_join, mock_read_parquet):
        """Test successful loading of player profile."""
        player_id = "test_player"
        expected_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        mock_read_parquet.return_value = expected_df
        mock_os_path_join.return_value = f"data/processed/player_{player_id}_profile.parquet"

        actual_df = load_player_profile(player_id)

        self.assertTrue(expected_df.equals(actual_df))
        mock_os_path_join.assert_called_once_with("data/processed", f"player_{player_id}_profile.parquet")
        mock_read_parquet.assert_called_once_with(f"data/processed/player_{player_id}_profile.parquet")

    @patch('pandas.read_parquet')
    @patch('os.path.join')
    def test_load_player_profile_file_not_found(self, mock_os_path_join, mock_read_parquet):
        """Test when the player profile file is not found."""
        player_id = "missing_player"
        mock_read_parquet.side_effect = FileNotFoundError()
        mock_os_path_join.return_value = f"data/processed/player_{player_id}_profile.parquet"

        result = load_player_profile(player_id)

        self.assertIsNone(result)
        mock_os_path_join.assert_called_once_with("data/processed", f"player_{player_id}_profile.parquet")
        mock_read_parquet.assert_called_once_with(f"data/processed/player_{player_id}_profile.parquet")
        self.assertLogs('src.data_access', level='ERROR')

    @patch('pandas.read_parquet')
    @patch('os.path.join')
    def test_load_player_profile_parquet_error(self, mock_os_path_join, mock_read_parquet):
        """Test when an error occurs during Parquet reading."""
        player_id = "error_player"
        mock_read_parquet.side_effect = ValueError("Parquet file is corrupted")
        mock_os_path_join.return_value = f"data/processed/player_{player_id}_profile.parquet"

        result = load_player_profile(player_id)

        self.assertIsNone(result)
        mock_os_path_join.assert_called_once_with("data/processed", f"player_{player_id}_profile.parquet")
        mock_read_parquet.assert_called_once_with(f"data/processed/player_{player_id}_profile.parquet")
        self.assertLogs('src.data_access', level='ERROR')

if __name__ == '__main__':
    unittest.main()
