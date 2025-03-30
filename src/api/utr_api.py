import requests
import os
import logging
from urllib.parse import quote
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get a logger instance for this module
logger = logging.getLogger(__name__)

class UTRAPI:
    """Handles authentication and API calls to UTR Sports."""

    # BASE_URL = "https://app.utrsports.net"
    # API_URL = "https://api.utrsports.net"
    # SEARCH_URL = f"{API_URL}/v2/search/players"
    # PROFILE_URL = f"{API_URL}/v1/player/{{player_id}}/profile"
    # RESULTS_URL = f"{API_URL}/v4/player/{{player_id}}/results"
    # STATS_URL = f"{API_URL}/v4/player/{{player_id}}/all-stats"

    def __init__(self):
        self.app_url = "https://app.utrsports.net"
        self.api_url = "https://api.utrsports.net"
        self.search_url = f"{self.api_url}/v2/search/players"
        self.profile_url = f"{self.api_url}/v1/player/{{player_id}}/profile"
        self.results_url = f"{self.api_url}/v4/player/{{player_id}}/results"
        self.stats_url = f"{self.api_url}/v4/player/{{player_id}}/all-stats"
        self.session = requests.Session()
        self.email = os.getenv("UTR_API_EMAIL")
        self.password = os.getenv("UTR_API_PASS")
        self.authenticated = False

    def _authenticate(self):
        """Logs into UTR Sports API."""
        self.session.get(self.app_url)
        auth_url = f"{self.app_url}/api/v1/auth/login"
        headers={"content-type": "application/json", "accept": "application/json"}
        payload={"email": self.email, "password": self.password}
        try:
            response = self.session.post(auth_url, json=payload, headers=headers)
            response.raise_for_status()  # Raise an exception for bad status codes
            logger.debug("Login successful.")
            self.authenticated = True
            return True
        except requests.exceptions.RequestException as e:
            logger.warning(f"Login failed: {response.status_code if 'response' in locals() else 'No Response'} - {e}")
            return False

    def _ensure_authenticated(self):
        """Ensures session is still valid."""
        if not self.authenticated:
            logger.info("Session expired or not initialized. Re-authenticating...")
            return self._authenticate
        return True

    def search_player(self, name):
        """Search for a player by name."""
        if not self._ensure_authenticated():
            logger.error("Re-authentication failed. Cannot perform search.")
            return None

        params={"query": name, "top": 40, "skip": 0, "utrType": "verified", "utrTeamType": "singles", "searchOrigin": "searchPage"}
        logger.debug(f"Searching for player '{name}' at: {self.search_url} with params: {params}")
        try:
            response = self.session.get(self.search_url, params=params)
            response.raise_for_status()
            data = response.json()
            players = data.get("hits", [])
            player_list = []
            for p in players:
                source = p.get("source", {})
                if not source:
                    continue
                location_data = source.get("location", {})
                location_display = location_data.get("display", "Unknown Location") if location_data else "Unknown Location"
                player_list.append({
                    "displayName": source.get("displayName", "Unknown"),
                    "id": source.get("id"),
                    "location": location_display,
                    })
            if not player_list:
                print("No players found.")
                return None

            # If multiple players, prompt user to select
            logger.debug(f"Search for '{name}' returned {len(player_list)} results.")
            if len(player_list) > 1:
                print("\nMultiple players found. Select one:")
                for idx, p in enumerate(player_list, 1):
                    print(f"{idx}. {p['displayName']} ({p['location']}) - ID: {p['id']}")

                while True:
                    user_input = input("Enter number (0 to cancel): ").strip()
                    if not user_input:
                        logger.info(f"No selection made, returning first player: {player_list[0]['displayName']} (ID: {player_list[0]['id']})")
                        return player_list[0]
                    try:
                        choice = int(user_input)
                        if choice == 0:
                            return None
                        if 1 <= choice <= len(player_list):
                            logger.info(f"User selected player: {player_list[choice - 1]['displayName']} (ID: {player_list[choice - 1]['id']})")
                            return player_list[choice - 1]
                    except ValueError:
                        pass
                    print("\nInvalid selection. Try again.")

            return player_list[0]

        except requests.exceptions.RequestException as e:
            logger.error(f"Search failed for '{name}' at {self.search_url}: {response.status_code if 'response' in locals() else 'No Response'} - {e}")
            return None

    def get_player_profile(self, player_id):
        """Fetch player's profile."""
        if not self._ensure_authenticated():
            logger.error(f"Re-authentication failed. Cannot fetch profile for player ID: {player_id}")
            return None
        profile_url = self.profile_url.format(player_id=player_id)
        logger.debug(f"Fetching profile for player ID '{player_id}' at: {profile_url}")
        try:
            response = self.session.get(profile_url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch profile for player ID '{player_id}' at {profile_url}: {response.status_code if 'response' in locals() else 'No Response'} - {e}")
            logger.debug(f"Request details: URL={profile_url}")
            return None

    def get_player_results(self, player_id):
        """Fetch player's match results."""
        if not self._ensure_authenticated():
            logger.error(f"Re-authentication failed. Cannot fetch results for player ID: {player_id}")
            return None
        results_url = self.results_url.format(player_id=player_id)
        try:
            response = self.session.get(results_url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch results for player ID '{player_id}' at {results_url}: {response.status_code if 'response' in locals() else 'No Response'} - {e}")
            return None

    def get_player_stats(self, player_id, stat="doubles"):
        """Fetch player's stats."""
        if not self._ensure_authenticated():
            logger.error(f"Re-authentication failed. Cannot fetch {stat} stats for player ID: {player_id}")
            return None
        stats_url = self.stats_url.format(player_id=player_id)
        params={"type": stat, "resultType": "verified", "months": 12, "fetchAllResults": "false"}
        logger.debug(f"Getting {stat} stats for '{player_id}' at: {self.stats_url} with params: {params}")
        try:
            response = self.session.get(stats_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {stat} stats for player ID '{player_id}' at {self.results_url}: {response.status_code if 'response' in locals() else 'No Response'} - {e}")
            return None
