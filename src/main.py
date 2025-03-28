import argparse
import logging.config
import logging
import os
import sys
from datetime import datetime
from api.utr_api import UTRAPI
from processing.data_saver import save_player_profile, save_player_results, save_player_stats
from analytics.utr_service import get_player_utr_scores # Import the UTR calculation function

def main():
    parser = argparse.ArgumentParser(description="UTR Player Lookup CLI")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--player", "-p", help="Player name to search")
    args = parser.parse_args()

    configure_logging(debug_mode=args.debug)
    logger = logging.getLogger("application")
    logger.debug("Debug mode enabled for application logger." if args.debug else "Debug mode not enabled.")

    api = UTRAPI()
    api_logger = logging.getLogger("api")
    analytics_logger = logging.getLogger("analytics")
    processing_logger = logging.getLogger("processing")
    data_access_logger = logging.getLogger("data_access")

    # Login
    api_logger.info("Attempting to log in to UTR API.")
    if not api._authenticate():
        api_logger.error("Failed to log in. Exiting.")
        print("Failed to log in. Exiting.")
        return

    while True:  # Loop to allow multiple searches
        if args.player:
            player_name = args.player
            logger.info(f"Searching for player from command line argument: '{player_name}'.")
            args.player = None  # Clear the argument for subsequent loops
        else:
            player_name = input("\nEnter player name to search (or 'q' to quit): ").strip()
            logger.info(f"User input: '{player_name}'.")

        if player_name.lower() == 'q':
            logger.info("User requested to quit the application.")
            break  # Exit the loop if the user enters 'q'

        # Search for player
        api_logger.info(f"Searching for player: '{player_name}'.")
        player = api.search_player(player_name)
        if not player:
            logger.info(f"No player found for search term: '{player_name}'.")
            print("No player selected. Try again.")
            continue  # Go to the next iteration of the loop

        player_id = player['id']
        player_display_name = player['displayName']
        logger.info(f"Found player: '{player_display_name}' (ID: {player_id}).")

        # Fetch player profile
        logger.info(f"Fetching profile for {player_display_name} (ID: {player_id}).")
        print(f"\nFetching profile for {player['displayName']}...")
        profile = api.get_player_profile(player['id'])
        if profile:
            processing_logger.info(f"Saving profile for {player_display_name} (ID: {player_id}).")
            save_player_profile(profile, player["id"])
            logger.info(f"Profile for {player_display_name} (ID: {player_id}) retrieved and saved successfully!")
            print(f"Profile for {player['displayName']} retrieved successfully!")
        else:
            logger.warning(f"Failed to retrieve profile for {player_display_name} (ID: {player_id}).")

        # Fetch player match results
        logger.info(f"Fetching results for {player_display_name} (ID: {player_id}).")
        print(f"\nFetching results for {player['displayName']}...")
        results = api.get_player_results(player['id'])
        if results:
            processing_logger.info(f"Saving results for {player_display_name} (ID: {player_id}).")
            save_player_results(results, player["id"])
            logger.info(f"Match results retrieved and saved for {player_display_name} (ID: {player_id}).")
            print(f"Match results retrieved for {player['displayName']}.")
        else:
            logger.warning(f"Failed to retrieve match results for {player_display_name} (ID: {player_id}).")

        # Fetch player stats
        print(f"\nFetching stats for {player['displayName']}...")
        logger.info(f"Fetching stats for {player_display_name} (ID: {player_id}).")
        logger.info(f"Fetching singles stats for {player_display_name} (ID: {player_id}).")
        stats_singles = api.get_player_stats(player_id, "singles")
        if stats_singles:
            processing_logger.info(f"Saving singles stats for {player_display_name} (ID: {player_id}).")
            save_player_stats(stats_singles, player_id, "singles")
            logger.info(f"Singles stats retrieved and saved for {player_display_name} (ID: {player_id}).")
        else:
            logger.warning(f"Failed to retrieve singles stats for {player_display_name} (ID: {player_id}).")

        logger.info(f"Fetching doubles stats for {player_display_name} (ID: {player_id}).")
        stats_doubles = api.get_player_stats(player_id, "doubles")
        if stats_doubles:
            processing_logger.info(f"Saving doubles stats for {player_display_name} (ID: {player_id}).")
            save_player_stats(stats_doubles, player_id, "doubles")
            logger.info(f"Doubles stats retrieved and saved for {player_display_name} (ID: {player_id}).")
        else:
            logger.warning(f"Failed to retrieve doubles stats for {player_display_name} (ID: {player_id}).")
        print(f"Match stats retrieved for {player_display_name}.")

        # Calculate UTR
        print(f"\nFetching player UTR for {player['displayName']}")
        logger.info(f"Fetching player UTR for {player_display_name} (ID: {player_id}).")
        analytics_logger.info(f"Calculating UTR scores for player ID: {player_id}.")
        utr_scores = get_player_utr_scores(player_id)
        if utr_scores:
            logger.info(f"UTR scores calculated for {player_display_name} (ID: {player_id}).")
        else:
            logger.warning(f"No UTR scores found for {player_display_name} (ID: {player_id}).")

        # Display results summary
        print("\nSummary:")
        print(f"Name: {player['displayName']}")
        print(f"Location: {player['location']}")
        print(f"Total Matches: {len(results.get('events', [])) if results else 0}")
        if utr_scores:
            sorted_matches = sorted(utr_scores.items(), key=lambda item: item[1]['date'], reverse=True)
            print("\nLast 3 UTR Scores:")
            for match_id, match_data in sorted_matches[:3]:
                date_object = datetime.strptime(match_data['date'], '%Y-%m-%dT%H:%M:%S')
                print(f"Match ID: {match_id}, Date: {date_object.strftime('%Y-%m-%d')}, UTR: {match_data['utr']}")
                # print(f"Match ID: {match_id}, UTR: {match_data['utr']}")
        else:
            print("\nNo UTR scores found.")
        print("-" * 40)
    logger.info("Application finished.")

def configure_logging(debug_mode=False):
    """Configures logging dynamically using dictConfig with separate log files."""
    log_level = logging.DEBUG if debug_mode else logging.INFO

    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(levelname)s - %(message)s",
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "simple",
                "stream": sys.stdout,
            },
            "application_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "application.log",
                "mode": "a",
            },
            "api_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "api.log",
                "mode": "a",
            },
            "analytics_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "analytics.log",
                "mode": "a",
            },
            "data_access_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "data_access.log",
                "mode": "a",
            },
            "processing_file": {
                "class": "logging.FileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": "processing.log",
                "mode": "a",
            },
        },
        "loggers": {
            "application": {
                "level": log_level,
                "handlers": ["application_file"],
                "propagate": False,
            },
            "analytics": {
                "level": log_level,
                "handlers": ["analytics_file"],
                "propagate": False,
            },
            "api": {
                "level": log_level,
                "handlers": ["api_file"],
                "propagate": False,
            },
            "data_access": {
                "level": log_level,
                "handlers": ["data_access_file"],
                "propagate": False,
            },
            "processing": {
                "level": log_level,
                "handlers": ["processing_file"],
                "propagate": False,
            },
        },
        "root": {
            "level": log_level,
            "handlers": ["console", "application_file"],
        },
    }

    logging.config.dictConfig(log_config)

if __name__ == "__main__":
    main()
