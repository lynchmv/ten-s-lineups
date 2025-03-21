import pandas as pd
import os
import json
import logging

DATA_DIR = "data/processed"

# Configure logging
logging.basicConfig(
    filename="data_saver.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def _save_parquet(df, file_path):
    """Helper function to save DataFrame to Parquet with error handling."""
    try:
        df.to_parquet(file_path, engine="pyarrow", index=False)
        logging.info(f"Saved DataFrame to {file_path}")
    except Exception as e:
        logging.error(f"Error saving DataFrame to {file_path}: {e}")

def save_profile_to_parquet(profile, player_id):
    """Save player profile to a Parquet file."""
    try:
        df = pd.json_normalize(profile)
        os.makedirs(DATA_DIR, exist_ok=True)
        file_path = os.path.join(DATA_DIR, f"player_{player_id}_profile.parquet")
        _save_parquet(df, file_path)
    except Exception as e:
        logging.error(f"Error saving profile for player {player_id}: {e}")

def _extract_results_data(results):
    """Extracts relevant data from the results JSON."""
    extracted_data = []
    for event in results.get("events", []):
        for draw in event.get("draws", []):
            for result in draw.get("results", []):
                # Extract relevant fields (adjust as needed)
                extracted_data.append({
                    "event_id": event.get("id"),
                    "event_name": event.get("name"),
                    "draw_id": draw.get("id"),
                    "draw_name": draw.get("name"),
                    "result_id": result.get("id"),
                    "date": result.get("date"),
                    # "winner_id": result.get("winner", {}).get("isWinner"),
                    # "loser_id": result.get("loser", {}).get("isWinner"),
                    "players": json.dumps(result.get("players", {})), #stringified player info.
                    "score": json.dumps(result.get("score", {})), #stringified score info.
                    "teamType": result.get("teamType"),
                    "sportTypeId": result.get("sportTypeId"),
                    "sourceType": result.get("sourceType"),
                    "completionType": result.get("completionType"),
                    "outcome": result.get("outcome"),
                    "finalized": result.get("finalized"),
                })
    return extracted_data

def validate_results_data(df):
    """Validates the results DataFrame."""
    if df.empty:
        logging.warning("Results DataFrame is empty.")
        return False
    if "date" not in df.columns:
        logging.error("Results DataFrame missing 'date' column.")
        return False
    # Add more validation checks as needed
    return True

def save_results_to_parquet(results, player_id):
    """Save player match results to a Parquet file, with extracted data and validation."""
    try:
        extracted_data = _extract_results_data(results)
        df = pd.DataFrame(extracted_data)

        if not validate_results_data(df):
            return  # Stop if validation fails

        os.makedirs(DATA_DIR, exist_ok=True)
        file_path = os.path.join(DATA_DIR, f"player_{player_id}_results.parquet")
        _save_parquet(df, file_path)
    except Exception as e:
        logging.error(f"Error saving results for player {player_id}: {e}")
