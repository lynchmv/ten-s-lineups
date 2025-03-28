import pandas as pd
import os
import json
import logging

DATA_DIR = "data/processed"
RAW_DIR = "data/raw"

# Get a logger instance for this module
logger = logging.getLogger(__name__)

def _save_parquet(df, file_path):
    """Helper function to save DataFrame to Parquet with error handling."""
    logger.debug(f"Saving DataFrame with {len(df)} rows to {file_path}")
    try:
        df.to_parquet(file_path, engine="pyarrow", index=False)
        logger.debug(f"Successfully saved DataFrame to {file_path}")
    except Exception as e:
        logger.error(f"Error saving DataFrame to {file_path}: {e}")

def _save_json(contents, file_path):
    """Helper function to save text to json with error handling."""
    logger.debug(f"Saving JSON data to {file_path}")
    try:
        with open(file_path, "w") as json_file:
            json.dump(contents, json_file, indent=2)
        logger.debug(f"Successfully saved raw json to {file_path}")
    except Exception as e:
        logger.error(f"Error saving raw json to {file_path}: {e}")

def save_player_profile(profile, player_id):
    """Save player profile to parquet file."""
    logger.info(f"Saving profile for player ID: {player_id}.")
    try:
        df = pd.json_normalize(profile)
        os.makedirs(DATA_DIR, exist_ok=True)
        file_path = os.path.join(DATA_DIR, f"player_{player_id}_profile.parquet")
        logger.info(f"Saving {player_id} profile to Parquet file.")
        _save_parquet(df, file_path)
    except Exception as e:
        logger.error(f"Error saving parquet profile for player {player_id}: {e}")

    """Save player profile to json file."""
    try:
        os.makedirs(RAW_DIR, exist_ok=True)
        file_path = os.path.join(RAW_DIR, f"player_{player_id}_profile.json")
        logger.info(f"Saving {player_id} profile to json file.")
        _save_json(profile, file_path)
    except Exception as e:
        logger.error(f"Error saving json profile for player {player_id}: {e}")

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
    logger.debug(f"Extracted {len(extracted_data)} results for processing.")
    return extracted_data

def validate_results_data(df):
    """Validates the results DataFrame."""
    if df.empty:
        logger.warning("Results DataFrame is empty.")
        return False
    if "date" not in df.columns:
        logger.error("Results DataFrame missing 'date' column.")
        return False
    logger.debug("Results DataFrame passed validation.")
    return True

def save_player_results(results, player_id):
    """Save player match results to a Parquet file, with extracted data and validation."""
    logger.info(f"Saving results for player ID: {player_id}.")
    try:
        extracted_data = _extract_results_data(results)
        df = pd.DataFrame(extracted_data)

        if not validate_results_data(df):
            return  # Stop if validation fails

        os.makedirs(DATA_DIR, exist_ok=True)
        file_path = os.path.join(DATA_DIR, f"player_{player_id}_results.parquet")
        logger.info(f"Saving {player_id} results DataFrame with {len(df)} rows to Parquet file.")
        _save_parquet(df, file_path)
    except Exception as e:
        logger.error(f"Error saving results for player {player_id}: {e}")

    """Save player results to json file."""
    try:
        os.makedirs(RAW_DIR, exist_ok=True)
        file_path = os.path.join(RAW_DIR, f"player_{player_id}_results.json")
        logger.info(f"Saving {player_id} results to json file.")
        _save_json(results, file_path)
    except Exception as e:
        logger.error(f"Error saving json results for player {player_id}: {e}")

def save_player_stats(stats, player_id, match="doubles"):
    """Save player stats to parquet file."""
    logger.info(f"Saving {match} stats for player ID: {player_id}.")
    try:
        df = pd.json_normalize(stats)
        os.makedirs(DATA_DIR, exist_ok=True)
        file_path = os.path.join(DATA_DIR, f"player_{player_id}_{match}_stats.parquet")
        logger.info(f"Saving {player_id} {match} stats DataFrame with {len(df)} rows to Parquet file.")
        _save_parquet(df, file_path)
    except Exception as e:
        logger.error(f"Error saving parquet stats for player {player_id}: {e}")

    """Save player stats to json file."""
    try:
        os.makedirs(RAW_DIR, exist_ok=True)
        file_path = os.path.join(RAW_DIR, f"player_{player_id}_{match}_stats.json")
        logger.info(f"Saved {player_id} {match} stats to json file.")
        _save_json(stats, file_path)
    except Exception as e:
        logger.error(f"Error saving json stats for player {player_id}: {e}")

