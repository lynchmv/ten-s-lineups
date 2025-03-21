import requests
import os
import logging
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

    def __init__(self):
        self.session = requests.Session()
        self.email = os.getenv("UTR_API_EMAIL")
        self.password = os.getenv("UTR_API_PASS")
        self.authenticated = False

        # Configure retry mechanism
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def login(self):
        """Logs into UTR Sports API."""
        self.session.get(self.BASE_URL)
        try:
            response = self.session.post(
                f"{self.BASE_URL}/api/v1/auth/login",
                json={"email": self.email, "password": self.password},
                headers={"content-type": "application/json", "accept": "application/json"},
            )
            response.raise_for_status()
            self.authenticated = True
            logging.info("Login successful.")
            return True
        except requests.exceptions.RequestException as e:
            logging.warning(f"Login failed: {e}")
            return False

    def ensure_session(self):
        """Ensures session is still valid."""
        if not self.authenticated:
            logging.info("Session expired. Re-authenticating...")
            if not self.login():
                logging.error("Re-authentication failed. Exiting.")
                raise RuntimeError("Failed to re-authenticate with UTR API")

    def _make_api_request(self, url, params=None):
        """Helper function for making API requests with error handling."""
        self.ensure_session()
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"API request failed: {e}")
            if response is not None:
                logging.error(f"Response text: {response.text}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            return None
        except ValueError as e:
            logging.error(f"Invalid json response: {e}")
            if response is not None:
                logging.error(f"Response text: {response.text}")
            return None

    def search_player(self, name):
        """Search for a player by name."""
        params = {"query": name, "top": 40, "skip": 0, "utrType": "verified", "utrTeamType": "singles", "searchOrigin": "searchPage"}
        data = self._make_api_request(self.SEARCH_URL, params=params)

        if not data:
            return None

        players = data.get("hits", [])
        player_list = []
        for p in players:
            source = p.get("source", {})
            if not source:
                continue
            player_list.append({
                "displayName": source.get("displayName", "Unknown"),
                "id": source.get("id"),
                "location": source.get("location", {}).get("display", "Unknown Location"),
            })

        if not player_list:
            print("No players found.")
            return None

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
        url = self.PROFILE_URL.format(player_id=player_id)
        return self._make_api_request(url)

    def get_player_results(self, player_id):
        """Fetch player's match results."""
        url = self.RESULTS_URL.format(player_id=player_id)
        return self._make_api_request(url)
