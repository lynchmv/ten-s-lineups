import argparse
import logging.config
import logging
import os
import sys
from datetime import datetime
import requests
from api.utr_api import UTRAPI
from processing.data_saver import save_player_profile, save_player_results, save_player_stats
from analytics.utr_service import get_player_utr_scores
from config import configure_logging

# Define the base URL for your backend API
API_BASE_URL = "http://tennis.lynuxss.com:8080/api"

def main(debug_mode=False):
    parser = argparse.ArgumentParser(description="UTR Player Lookup and Importer CLI")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--player", "-p", help="Player name to search")
    args = parser.parse_args()

    configure_logging(debug_mode)
    logger = logging.getLogger("application")
    logger.info("Application started.")
    logger.debug("Debug mode enabled for application logger." if args.debug else "Debug mode not enabled.")

    api = UTRAPI()
    api_logger = logging.getLogger("api")
    analytics_logger = logging.getLogger("analytics")
    processing_logger = logging.getLogger("processing")

    # Login
    api_logger.info("Attempting to log in to UTR API.")
    if not api._authenticate():
        api_logger.error("Failed to log in. Exiting.")
        print("Failed to log in. Exiting.")
        return

    while True:
        if args.player:
            player_name = args.player
            logger.info(f"Searching for player from command line argument: '{player_name}'.")
            args.player = None
        else:
            player_name = input("\nEnter player name to search (or 'q' to quit): ").strip()
            logger.info(f"User input: '{player_name}'.")

        if player_name.lower() == 'q':
            logger.info("User requested to quit the application.")
            break

        # Search for player
        api_logger.info(f"Searching for player: '{player_name}'.")
        player = api.search_player(player_name)
        if not player:
            logger.info(f"No player found for search term: '{player_name}'.")
            print("No player selected. Try again.")
            continue

        player_id = player['id']
        player_display_name = player['displayName']
        logger.info(f"Found player: '{player_display_name}' (ID: {player_id}).")

        # Fetch player profile and send to API
        logger.info(f"Fetching profile for {player_display_name} (ID: {player_id}).")
        print(f"\nFetching profile for {player['displayName']}...")
        profile = api.get_player_profile(player['id'])
        if profile:
            try:
                processing_logger.info(f"Sending profile for {player_display_name} to backend API.")
                response = requests.post(f"{API_BASE_URL}/players", json=profile)
                response.raise_for_status()  # This will raise an exception for HTTP errors
                print(f"Profile for {player['displayName']} sent to application successfully!")
                logger.info(f"Profile for {player_display_name} sent successfully.")
            except requests.exceptions.RequestException as e:
                print(f"Error sending profile to API: {e}")
                logger.error(f"API Error sending profile for {player_display_name}: {e}")
        else:
            logger.warning(f"Failed to retrieve profile for {player_display_name} (ID: {player_id}).")


        # Fetch player match results and send to API
        logger.info(f"Fetching results for {player_display_name} (ID: {player_id}).")
        print(f"\nFetching results for {player['displayName']}...")
        results = api.get_player_results(player['id'])
        if results:
            try:
                processing_logger.info(f"Sending results for {player_display_name} to backend API.")
                response = requests.post(f"{API_BASE_URL}/matches/import", json=results)
                response.raise_for_status()
                import_summary = response.json()
                print(f"Match results for {player['displayName']} sent to application successfully!")
                print(f"Summary: {import_summary.get('imported', 0)} imported, {import_summary.get('skipped', 0)} skipped.")
                logger.info(f"Results for {player_display_name} sent successfully: {import_summary}")
            except requests.exceptions.RequestException as e:
                print(f"Error sending results to API: {e}")
                logger.error(f"API Error sending results for {player_display_name}: {e}")
        else:
            logger.warning(f"Failed to retrieve match results for {player_display_name} (ID: {player_id}).")

        print("-" * 40)
    logger.info("Application finished.")


if __name__ == "__main__":
    debug = "--debug" in sys.argv
    main(debug)
    sys.exit(0)
