import requests
import os
import logging
from urllib.parse import quote
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename="utr_api.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class UTRAPI:
    """Handles authentication and API calls to UTR Sports."""

    BASE_URL = "https://app.utrsports.net"
    API_URL = "https://api.utrsports.net"
    SEARCH_URL = f"{API_URL}/v2/search/players"
    PROFILE_URL = f"{API_URL}/v1/player/{{player_id}}/profile"
    RESULTS_URL = f"{API_URL}/v4/player/{{player_id}}/results"
    STATS_URL = f"{API_URL}/v4/player/{{player_id}}/all-stats"

    def __init__(self):
        self.session = requests.Session()
        self.email = os.getenv("UTR_API_EMAIL")
        self.password = os.getenv("UTR_API_PASS")
        self.authenticated = False

    def login(self):
        """Logs into UTR Sports API."""
        self.session.get(self.BASE_URL)
        response = self.session.post(
            f"{self.BASE_URL}/api/v1/auth/login",
            json={"email": self.email, "password": self.password},
            headers={"content-type": "application/json", "accept": "application/json"}
        )

        if response.status_code == 200:
            self.authenticated = True
            logging.info("Login successful.")
            return True
        else:
            logging.warning(f"Login failed: {response.status_code}")
            return False

    def ensure_session(self):
        """Ensures session is still valid."""
        if not self.authenticated:
            logging.info("Session expired. Re-authenticating...")
            if not self.login():  # If login fails, notify and exit early
                logging.error("Re-authentication failed. Exiting.")
                raise RuntimeError("Failed to re-authenticate with UTR API")

    def search_player(self, name):
        """Search for a player by name."""
        self.ensure_session()

        response = self.session.get(self.SEARCH_URL, params={"query": name, "top": 40, "skip": 0, "utrType": "verified", "utrTeamType": "singles", "searchOrigin": "searchPage"})

        if response.status_code != 200:
            logging.error(f"Search failed: {response.status_code}")
            return None

        data = response.json()
        players = data.get("hits", [])

        # Extract necessary fields
        player_list = []
        for p in players:
            source = p.get("source", {})
            if not source:
                continue
            player_list.append({
                "displayName": source.get("displayName", "Unknown"),
                "id": source.get("id"),
                "location": source.get("location", {}).get("display", "Unknown Location")
            })

        if not player_list:
            print("No players found.")
            return None

        # If multiple players, prompt user to select
        logging.info(f"Search for '{name}' returned {len(player_list)} results.")
        if len(player_list) > 1:
            print("\nMultiple players found. Select one:")
            for idx, p in enumerate(player_list, 1):
                print(f"{idx}. {p['displayName']} ({p['location']}) - ID: {p['id']}")

            while True:
                try:
                    choice = int(input("Enter number (0 to cancel): "))
                    if choice == 0:
                        return None
                    if 1 <= choice <= len(player_list):
                        logging.info(f"User selected player: {player_list[choice - 1]['displayName']} (ID: {player_list[choice - 1]['id']})")
                        return player_list[choice - 1]
                except ValueError:
                    pass
                print("Invalid selection. Try again.")


        return player_list[0]

    def get_player_profile(self, player_id):
        """Fetch player's profile."""
        self.ensure_session()
        response = self.session.get(self.PROFILE_URL.format(player_id=player_id))
        return response.json() if response.status_code == 200 else None

    def get_player_results(self, player_id):
        """Fetch player's match results."""
        self.ensure_session()
        response = self.session.get(self.RESULTS_URL.format(player_id=player_id))
        return response.json() if response.status_code == 200 else None

    def get_player_stats(self, player_id, stat="doubles"):
        """Fetch player's stats."""
        self.ensure_session()
        response = self.session.get(self.STATS_URL.format(player_id=player_id), params={"type": stat, "resultType": "verified", "months": 12, "fetchAllResults": "false"})
        return response.json() if response.status_code == 200 else None
