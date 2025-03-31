import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from src.analytics.utr_service import get_player_utr_scores
import json

class TestUTRService(unittest.TestCase):

    def test_get_player_utr_scores_no_results(self):
        """Test get_player_utr_scores when no player results are found."""
        mock_load_results = MagicMock(return_value=pd.DataFrame())

        with patch('src.analytics.utr_service.load_player_results', mock_load_results):
            player_id = "test_player"
            result = get_player_utr_scores(player_id)
            self.assertEqual(result, {})
            mock_load_results.assert_called_once_with(player_id)

    @patch('src.analytics.utr_service.load_player_results')
    @patch('src.analytics.utr_service.get_match_utr')
    def test_get_player_utr_scores_singles_matches(self, mock_get_match_utr, mock_load_results):
        """Test get_player_utr_scores with only singles matches."""
        player_id = "test_player"
        mock_data = pd.DataFrame({
            'event_id': ['match1', 'match2'],
            'players': [
                json.dumps({"winner1": "player_a", "loser1": "player_b", "winner2": None, "loser2": None}),
                json.dumps({"winner1": "player_c", "loser1": "player_d", "winner2": None, "loser2": None})
            ],
            'date': ['2025-03-20T10:00:00Z', '2025-03-25T15:00:00Z'],
            'event_name': ['Singles Event A', 'Singles Event B'] # Added event_name as it's used in get_match_utr
        })
        mock_load_results.return_value = mock_data

        # Now we need to mock get_match_utr to return specific UTRs for these matches
        mock_get_match_utr.side_effect = [10.5, 11.2]  # UTRs for match1 and match2 respectively

        result = get_player_utr_scores(player_id)

        expected_result = {
            'match1': {'utr': 10.5, 'date': '2025-03-20T10:00:00Z'},
            'match2': {'utr': 11.2, 'date': '2025-03-25T15:00:00Z'}
        }
        self.assertEqual(result, expected_result)

        # Assert that our mocks were called correctly
        mock_load_results.assert_called_once_with(player_id)
        self.assertEqual(mock_get_match_utr.call_count, 2)
        mock_get_match_utr.assert_any_call(player_id, {'descriptions': [{'resultDate': '2025-03-20T10:00:00Z', 'details': 'Singles Event A'}]}, 'singles')
        mock_get_match_utr.assert_any_call(player_id, {'descriptions': [{'resultDate': '2025-03-25T15:00:00Z', 'details': 'Singles Event B'}]}, 'singles')

    @patch('src.analytics.utr_service.load_player_results')
    @patch('src.analytics.utr_service.get_match_utr')
    def test_get_player_utr_scores_doubles_matches(self, mock_get_match_utr, mock_load_results):
        """Test get_player_utr_scores with only doubles matches."""
        player_id = "test_player"
        mock_data = pd.DataFrame({
            'event_id': ['match1', 'match2'],
            'players': [
                json.dumps({"winner1": "player_a", "loser1": "player_b", "winner2": "player_c", "loser2": "player_d"}),
                json.dumps({"winner1": "player_c", "loser1": "player_d", "winner2": "player_e", "loser2": "player_f"})
            ],
            'date': ['2025-03-20T10:00:00Z', '2025-03-25T15:00:00Z'],
            'event_name': ['Doubles Event A', 'Doubles Event B'] # Added event_name as it's used in get_match_utr
        })
        mock_load_results.return_value = mock_data

        # Now we need to mock get_match_utr to return specific UTRs for these matches
        mock_get_match_utr.side_effect = [8.2, 9.5]  # UTRs for match1 and match2 respectively

        result = get_player_utr_scores(player_id)

        expected_result = {
            'match1': {'utr': 8.2, 'date': '2025-03-20T10:00:00Z'},
            'match2': {'utr': 9.5, 'date': '2025-03-25T15:00:00Z'}
        }
        self.assertEqual(result, expected_result)

        # Assert that our mocks were called correctly
        mock_load_results.assert_called_once_with(player_id)
        self.assertEqual(mock_get_match_utr.call_count, 2)
        mock_get_match_utr.assert_any_call(player_id, {'descriptions': [{'resultDate': '2025-03-20T10:00:00Z', 'details': 'Doubles Event A'}]}, 'doubles')
        mock_get_match_utr.assert_any_call(player_id, {'descriptions': [{'resultDate': '2025-03-25T15:00:00Z', 'details': 'Doubles Event B'}]}, 'doubles')

    @patch('src.analytics.utr_service.load_player_results')
    @patch('src.analytics.utr_service.get_match_utr')
    def test_get_player_utr_scores_mixed_matches(self, mock_get_match_utr, mock_load_results):
        """Test get_player_utr_scores with a mix of singles and doubles matches."""
        player_id = "test_player"
        mock_data = pd.DataFrame({
            'event_id': ['match1', 'match2'],
            'players': [
                json.dumps({"winner1": "player_a", "loser1": "player_b", "winner2": None, "loser2": None}),  # Singles
                json.dumps({"winner1": "player_c", "loser1": "player_d", "winner2": "player_e", "loser2": "player_f"})   # Doubles
            ],
            'date': ['2025-03-20T10:00:00Z', '2025-03-25T15:00:00Z'],
            'event_name': ['Singles Event', 'Doubles Event']
        })
        mock_load_results.return_value = mock_data

        # Mock get_match_utr to return different UTRs for each match
        mock_get_match_utr.side_effect = [10.2, 8.9]  # UTRs for singles and doubles, respectively

        result = get_player_utr_scores(player_id)

        expected_result = {
            'match1': {'utr': 10.2, 'date': '2025-03-20T10:00:00Z'},
            'match2': {'utr': 8.9, 'date': '2025-03-25T15:00:00Z'}
        }
        self.assertEqual(result, expected_result)

        # Assert that our mocks were called correctly
        mock_load_results.assert_called_once_with(player_id)
        self.assertEqual(mock_get_match_utr.call_count, 2)

        # Assert calls to get_match_utr with correct match_type
        calls = [
            call(player_id, {'descriptions': [{'resultDate': '2025-03-20T10:00:00Z', 'details': 'Singles Event'}]}, 'singles'),
            call(player_id, {'descriptions': [{'resultDate': '2025-03-25T15:00:00Z', 'details': 'Doubles Event'}]}, 'doubles')
        ]
        mock_get_match_utr.assert_has_calls(calls, any_order=False)

    @patch('src.analytics.utr_service.load_player_results')
    @patch('src.analytics.utr_service.get_match_utr')
    def test_get_player_utr_scores_get_match_utr_returns_none(self, mock_get_match_utr, mock_load_results):
        """Test get_player_utr_scores when get_match_utr returns None for a match."""
        player_id = "test_player"
        mock_data = pd.DataFrame({
            'event_id': ['match1', 'match2', 'match3'],
            'players': [
                json.dumps({"winner1": "a", "loser1": "b", "winner2": None, "loser2": None}),  # Singles
                json.dumps({"winner1": "c", "loser1": "d", "winner2": "e", "loser2": "f"}),   # Doubles
                json.dumps({"winner1": "g", "loser1": "h", "winner2": None, "loser2": None})   # Singles
            ],
            'date': ['2025-03-20T10:00:00Z', '2025-03-25T15:00:00Z', '2025-04-01T12:00:00Z'],
            'event_name': ['Singles A', 'Doubles B', 'Singles C']
        })
        mock_load_results.return_value = mock_data

        # Simulate get_match_utr returning a UTR for the first and third matches, but None for the second
        mock_get_match_utr.side_effect = [10.1, None, 9.5]

        result = get_player_utr_scores(player_id)

        expected_result = {
            'match1': {'utr': 10.1, 'date': '2025-03-20T10:00:00Z'},
            'match3': {'utr': 9.5, 'date': '2025-04-01T12:00:00Z'}
        }
        self.assertEqual(result, expected_result)

        # Assert that our mocks were called correctly
        mock_load_results.assert_called_once_with(player_id)
        self.assertEqual(mock_get_match_utr.call_count, 3)

        calls = [
            call(player_id, {'descriptions': [{'resultDate': '2025-03-20T10:00:00Z', 'details': 'Singles A'}]}, 'singles'),
            call(player_id, {'descriptions': [{'resultDate': '2025-03-25T15:00:00Z', 'details': 'Doubles B'}]}, 'doubles'),
            call(player_id, {'descriptions': [{'resultDate': '2025-04-01T12:00:00Z', 'details': 'Singles C'}]}, 'singles')
        ]
        mock_get_match_utr.assert_has_calls(calls, any_order=False)

    @patch('src.analytics.utr_service.load_player_results')
    @patch('src.analytics.utr_service.get_match_utr')
    def test_get_player_utr_scores_load_results_returns_none(self, mock_get_match_utr, mock_load_results):
        """Test get_player_utr_scores when load_player_results returns None."""
        player_id = "test_player"
        mock_load_results.return_value = None

        result = get_player_utr_scores(player_id)

        self.assertEqual(result, {})  # Expect an empty dict as per the function's logic
        mock_load_results.assert_called_once_with(player_id)
        mock_get_match_utr.assert_not_called()

    @patch('src.analytics.utr_service.load_player_results')
    @patch('src.analytics.utr_service.get_match_utr')
    def test_get_player_utr_scores_load_results_raises_exception(self, mock_get_match_utr, mock_load_results):
        """Test get_player_utr_scores when load_player_results raises an exception."""
        player_id = "test_player"
        mock_load_results.side_effect = FileNotFoundError("Could not load results")

        result = get_player_utr_scores(player_id)

        self.assertIsNone(result)  # Expect None due to the try-except block
        mock_load_results.assert_called_once_with(player_id)
        mock_get_match_utr.assert_not_called()

if __name__ == '__main__':
    unittest.main()
