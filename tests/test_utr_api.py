import unittest
from src.api.utr_api import UTRAPI
from unittest.mock import patch, MagicMock
import requests
import json

class TestUTRAPI(unittest.TestCase):

    def setUp(self):
        """Set up for test methods."""
        self.api = UTRAPI()
        self.api.email = "test_user"  # Avoid using real credentials in tests
        self.api.password = "test_password"

    def tearDown(self):
        """Clean up after test methods."""
        pass

    @patch('requests.Session.post')
    def test_authentication_successful(self, mock_post):
        """Test successful authentication."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None  # Simulate successful status
        mock_post.return_value = mock_response

        result = self.api._authenticate()
        self.assertTrue(result)
        self.assertTrue(self.api.authenticated)
        mock_post.assert_called_once()

    @patch('requests.Session.post')
    def test_authentication_failure_bad_status(self, mock_post):
        """Test authentication failure due to a bad status code."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Unauthorized")
        mock_post.return_value = mock_response

        result = self.api._authenticate()
        self.assertFalse(result)
        self.assertFalse(self.api.authenticated)
        mock_post.assert_called_once()

    @patch('requests.Session.post')
    def test_authentication_failure_connection_error(self, mock_post):
        """Test authentication failure due to a connection error."""
        mock_post.side_effect = requests.exceptions.ConnectionError("Simulated connection error")

        result = self.api._authenticate()
        self.assertFalse(result)
        self.assertFalse(self.api.authenticated)
        mock_post.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_search_player_successful_single_result(self, mock_get, mock_ensure_auth):
        """Test successful search returning a single player."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": [{
                "source": {
                    "displayName": "John Doe",
                    "id": "johndoe123",
                    "location": {"display": "New York, USA"}
                }
            }],
            "total": 1
        }
        mock_get.return_value = mock_response

        player_name = "John Doe"
        result = self.api.search_player(player_name)

        self.assertIsNotNone(result)
        self.assertEqual(result['displayName'], "John Doe")
        self.assertEqual(result['id'], "johndoe123")
        self.assertEqual(result['location'], "New York, USA")
        mock_get.assert_called_once()
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_search_player_successful_multiple_results_select_first(self, mock_get, mock_ensure_auth):
        """Test successful search returning multiple players (implicitly selects the first)."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": [
                {"source": {"displayName": "Jane Smith", "id": "janesmith456", "location": {"display": "London, UK"}}},
                {"source": {"displayName": "Jane Smith", "id": "janesmith789", "location": {"display": "Paris, France"}}}
            ],
            "total": 2
        }
        mock_get.return_value = mock_response

        player_name = "Jane Smith"
        # We need to simulate the user not providing input to select, so the first result is returned
        with patch('builtins.input', return_value=''):
            result = self.api.search_player(player_name)

        self.assertIsNotNone(result)
        self.assertEqual(result['displayName'], "Jane Smith")
        self.assertEqual(result['id'], "janesmith456")
        self.assertEqual(result['location'], "London, UK")
        mock_get.assert_called_once()
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_search_player_no_results(self, mock_get, mock_ensure_auth):
        """Test search returning no players."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": [], "total": 0}
        mock_get.return_value = mock_response

        player_name = "Nonexistent Player"
        result = self.api.search_player(player_name)

        self.assertIsNone(result)
        mock_get.assert_called_once()
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_search_player_api_error(self, mock_get, mock_ensure_auth):
        """Test search when the API returns an error status code."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_get.return_value = mock_response

        player_name = "Error Name"
        result = self.api.search_player(player_name)

        self.assertIsNone(result)
        mock_get.assert_called_once()
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_search_player_connection_error(self, mock_get, mock_ensure_auth):
        """Test search when a connection error occurs."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Simulated connection error")

        player_name = "Connection Error Name"
        result = self.api.search_player(player_name)

        self.assertIsNone(result)
        mock_get.assert_called_once()
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=False)
    def test_search_player_not_authenticated(self, mock_ensure_auth):
        """Test that search_player handles unauthenticated state."""
        player_name = "Any Name"
        result = self.api.search_player(player_name)

        self.assertIsNone(result)
        mock_ensure_auth.assert_called_once()
        # We don't assert mock_get here because it shouldn't be called if not authenticated

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_profile_successful(self, mock_get, mock_ensure_auth):
        """Test successful retrieval of a player profile."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "johndoe123", "displayName": "John Doe", "utr": 12.5}
        mock_get.return_value = mock_response

        player_id = "johndoe123"
        result = self.api.get_player_profile(player_id)

        self.assertIsNotNone(result)
        self.assertEqual(result['id'], "johndoe123")
        self.assertEqual(result['displayName'], "John Doe")
        self.assertEqual(result['utr'], 12.5)
        mock_get.assert_called_once_with(self.api.profile_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_profile_not_found(self, mock_get, mock_ensure_auth):
        """Test when the API returns a 404 Not Found error."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
        mock_get.return_value = mock_response

        player_id = "nonexistentplayer"
        result = self.api.get_player_profile(player_id)

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.profile_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_profile_api_error(self, mock_get, mock_ensure_auth):
        """Test when the API returns a 500 Server Error."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_get.return_value = mock_response

        player_id = "someplayer"
        result = self.api.get_player_profile(player_id)

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.profile_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_profile_connection_error(self, mock_get, mock_ensure_auth):
        """Test when a connection error occurs while fetching the profile."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Simulated connection error")

        player_id = "anotherplayer"
        result = self.api.get_player_profile(player_id)

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.profile_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=False)
    def test_get_player_profile_not_authenticated(self, mock_ensure_auth):
        """Test that get_player_profile handles unauthenticated state."""
        player_id = "anyplayer"
        result = self.api.get_player_profile(player_id)

        self.assertIsNone(result)
        mock_ensure_auth.assert_called_once()
        # We don't assert mock_get here because it shouldn't be called if not authenticated

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_results_successful(self, mock_get, mock_ensure_auth):
        """Test successful retrieval of player results."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"events": [{"name": "Match 1"}, {"name": "Match 2"}]}
        mock_get.return_value = mock_response

        player_id = "someplayer"
        result = self.api.get_player_results(player_id)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertIn("events", result)
        self.assertEqual(len(result["events"]), 2)
        mock_get.assert_called_once_with(self.api.results_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_results_no_results(self, mock_get, mock_ensure_auth):
        """Test when the API returns no results for the player."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"events": []}
        mock_get.return_value = mock_response

        player_id = "anotherplayer"
        result = self.api.get_player_results(player_id)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["events"], [])
        mock_get.assert_called_once_with(self.api.results_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_results_api_error(self, mock_get, mock_ensure_auth):
        """Test when the API returns a 500 Server Error."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_get.return_value = mock_response

        player_id = "errorplayer"
        result = self.api.get_player_results(player_id)

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.results_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_results_not_found(self, mock_get, mock_ensure_auth):
        """Test when the API returns a 404 Not Found error."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
        mock_get.return_value = mock_response

        player_id = "notfoundplayer"
        result = self.api.get_player_results(player_id)

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.results_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_results_connection_error(self, mock_get, mock_ensure_auth):
        """Test when a connection error occurs while fetching results."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Simulated connection error")

        player_id = "connectionerrorplayer"
        result = self.api.get_player_results(player_id)

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.results_url.format(player_id=player_id))
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=False)
    def test_get_player_results_not_authenticated(self, mock_ensure_auth):
        """Test that get_player_results handles unauthenticated state."""
        player_id = "anyplayer"
        result = self.api.get_player_results(player_id)

        self.assertIsNone(result)
        mock_ensure_auth.assert_called_once()
        # We don't assert mock_get here because it shouldn't be called if not authenticated

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_successful_doubles(self, mock_get, mock_ensure_auth):
        """Test successful retrieval of doubles stats."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"stats": [{"type": "doubles", "wins": 10}]}
        mock_get.return_value = mock_response

        player_id = "someplayer"
        result = self.api.get_player_stats(player_id, "doubles")

        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertIn("stats", result)
        self.assertEqual(result["stats"][0]["type"], "doubles")
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "doubles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_successful_singles(self, mock_get, mock_ensure_auth):
        """Test successful retrieval of singles stats."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"stats": [{"type": "singles", "wins": 15}]}
        mock_get.return_value = mock_response

        player_id = "anotherplayer"
        result = self.api.get_player_stats(player_id, "singles")

        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertIn("stats", result)
        self.assertEqual(result["stats"][0]["type"], "singles")
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "singles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_no_results_doubles(self, mock_get, mock_ensure_auth):
        """Test when the API returns no doubles stats."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"stats": []}
        mock_get.return_value = mock_response

        player_id = "noplaysdoubles"
        result = self.api.get_player_stats(player_id, "doubles")

        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["stats"], [])
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "doubles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_no_results_singles(self, mock_get, mock_ensure_auth):
        """Test when the API returns no singles stats."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"stats": []}
        mock_get.return_value = mock_response

        player_id = "noplayssingles"
        result = self.api.get_player_stats(player_id, "singles")

        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["stats"], [])
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "singles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_api_error_doubles(self, mock_get, mock_ensure_auth):
        """Test API error (500) when fetching doubles stats."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_get.return_value = mock_response

        player_id = "errorplayer"
        result = self.api.get_player_stats(player_id, "doubles")

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "doubles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_api_error_singles(self, mock_get, mock_ensure_auth):
        """Test API error (500) when fetching singles stats."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_get.return_value = mock_response

        player_id = "errorplayer"
        result = self.api.get_player_stats(player_id, "singles")

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "singles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_not_found_doubles(self, mock_get, mock_ensure_auth):
        """Test Not Found (404) when fetching doubles stats."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
        mock_get.return_value = mock_response

        player_id = "notfoundplayer"
        result = self.api.get_player_stats(player_id, "doubles")

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "doubles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_not_found_singles(self, mock_get, mock_ensure_auth):
        """Test Not Found (404) when fetching singles stats."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
        mock_get.return_value = mock_response

        player_id = "notfoundplayer"
        result = self.api.get_player_stats(player_id, "singles")

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "singles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_connection_error_doubles(self, mock_get, mock_ensure_auth):
        """Test connection error when fetching doubles stats."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Simulated connection error")

        player_id = "connectionerrorplayer"
        result = self.api.get_player_stats(player_id, "doubles")

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "doubles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=True)
    @patch('requests.Session.get')
    def test_get_player_stats_connection_error_singles(self, mock_get, mock_ensure_auth):
        """Test connection error when fetching singles stats."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Simulated connection error")

        player_id = "connectionerrorplayer"
        result = self.api.get_player_stats(player_id, "singles")

        self.assertIsNone(result)
        mock_get.assert_called_once_with(self.api.stats_url.format(player_id=player_id), params={"type": "singles", "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        mock_ensure_auth.assert_called_once()

    @patch('src.api.utr_api.UTRAPI._ensure_authenticated', return_value=False)
    def test_get_player_stats_not_authenticated(self, mock_ensure_auth):
        """Test that get_player_stats handles unauthenticated state."""
        player_id = "anyplayer"
        result_doubles = self.api.get_player_stats(player_id, "doubles")
        result_singles = self.api.get_player_stats(player_id, "singles")

        self.assertIsNone(result_doubles)
        self.assertIsNone(result_singles)
        self.assertEqual(mock_ensure_auth.call_count, 2)
        # We don't assert mock_get here because it shouldn't be called if not authenticated

if __name__ == '__main__':
    unittest.main()
