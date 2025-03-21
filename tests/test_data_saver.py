import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import pandas as pd
from processing.data_saver import save_profile_to_parquet, save_results_to_parquet

class TestDataSaver(unittest.TestCase):
    def setUp(self):
        """Create a temp directory for tests"""
        self.test_dir = "tests/temp_data"
        os.makedirs(self.test_dir, exist_ok=True)

    def tearDown(self):
        """Clean up test files"""
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)

    def test_save_profile_to_parquet(self):
        """Test saving player profile to Parquet"""
        profile_data = {"player": {"name": "John Doe", "rank": 5}}
        player_id = "1234"

        file_path = os.path.join(self.test_dir, f"player_{player_id}_profile.parquet")
        save_profile_to_parquet(profile_data, player_id)

        # Check if file was created
        expected_path = os.path.join(os.getcwd(), "data/processed/player_1234_profile.parquet")
        print(f"Checking for file: {expected_path}")
        self.assertTrue(os.path.exists(expected_path))

        # Check if data is correctly saved
        df = pd.read_parquet(expected_path)
        self.assertIn("player.name", df.columns)
        self.assertEqual(df.iloc[0]["player.name"], "John Doe")

    def test_save_results_to_parquet(self):
        """Test saving player match results to Parquet"""
        results_data = {"results": [{"match_id": 1, "score": "6-3, 6-4"}]}
        player_id = "1234"

        file_path = os.path.join(self.test_dir, f"player_{player_id}_results.parquet")
        save_results_to_parquet(results_data, player_id)

        # Check if file was created
        expected_path = os.path.join(os.getcwd(), "data/processed/player_1234_results.parquet")
        print(f"Checking for file: {expected_path}")
        self.assertTrue(os.path.exists(expected_path))

        # Check if data is correctly saved
        df = pd.read_parquet(expected_path)
        self.assertIn("match_id", df.columns)
        self.assertEqual(df.iloc[0]["score"], "6-3, 6-4")

if __name__ == "__main__":
    unittest.main()

