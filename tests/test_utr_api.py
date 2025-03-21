import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from api.utr_api import UTRAPI

class TestUTRAPI(unittest.TestCase):
    @patch("requests.Session.post")
    def test_login_success(self, mock_post):
        """Test successful login"""
        mock_post.return_value.status_code = 200
        api = UTRAPI()
        self.assertTrue(api.login())

    @patch("requests.Session.post")
    def test_login_failure(self, mock_post):
        """Test failed login"""
        mock_post.return_value.status_code = 401
        api = UTRAPI()
        self.assertFalse(api.login())

    @patch("requests.Session.get")
    def test_search_player(self, mock_get):
        """Test searching for a player"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "hits": [
                {"source": {"displayName": "John Doe", "id": "1234", "location": {"display": "New York"}}}
            ]
        }

        api = UTRAPI()
        player = api.search_player("John Doe")
        self.assertEqual(player["displayName"], "John Doe")
        self.assertEqual(player["id"], "1234")

    @patch("requests.Session.get")
    def test_get_player_profile(self, mock_get):
        """Test fetching player profile"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"player": {"name": "John Doe", "rank": 5}}

        api = UTRAPI()
        profile = api.get_player_profile("1234")
        self.assertEqual(profile["player"]["name"], "John Doe")

    @patch("requests.Session.get")
    def test_get_player_results(self, mock_get):
        """Test fetching player match results"""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"results": [{"match_id": 1, "score": "6-3, 6-4"}]}

        api = UTRAPI()
        results = api.get_player_results("1234")
        self.assertEqual(len(results["results"]), 1)
        self.assertEqual(results["results"][0]["score"], "6-3, 6-4")

if __name__ == "__main__":
    unittest.main()

