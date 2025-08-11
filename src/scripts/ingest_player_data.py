import os
from src.player.player_data_loader import load_player_match_stats, DB_DIR, PLAYERS_DB_FILE
from datetime import datetime
from src.player.player_data_loader import load_player_match_stats, DB_DIR, PLAYERS_DB_FILE

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config import configure_logging
import logging

RAW_DATA_DIR = "data/raw"

if __name__ == "__main__":
    # Configure logging
    configure_logging()
    logger = logging.getLogger("ingest_data")  # Use the specific logger for this script

    logger.info("Starting player match data ingestion...")

    # Create the database directory if it doesn't exist
    os.makedirs(DB_DIR, exist_ok=True)

    # Iterate through files in the raw data directory
    for filename in os.listdir(RAW_DATA_DIR):
        if filename.startswith("player_") and filename.endswith("_results.json"):
            try:
                # Extract the player ID from the filename
                player_id = filename.split("_")[1]
                file_path = os.path.join(RAW_DATA_DIR, filename)
                logger.info(f"Processing file: {filename} for player ID: {player_id}")
                load_player_match_stats(player_id, file_path)
                logger.info(f"Successfully processed data for player ID: {player_id}")
            except Exception as e:
                logger.error(f"Error processing file {filename}: {e}")

    logger.info("Player match data ingestion complete.")
    logger.info(f"Match data should be in: {PLAYERS_DB_FILE}")
