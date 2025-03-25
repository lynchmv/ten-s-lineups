import pandas as pd
import json
import os
import logging

DATA_DIR = "data/processed"
RAW_DIR = "data/raw"

def load_player_results(player_id):
    """Loads player results from a Parquet file."""
    try:
        results_path = os.path.join(DATA_DIR, f"player_{player_id}_results.parquet")
        return pd.read_parquet(results_path)
    except FileNotFoundError:
        logging.error(f"Player results file not found for player {player_id}")
        return None
    except Exception as e:
        logging.error(f"Error loading player results: {e}")
        return None

def load_player_stats(player_id, match_type):
    """Loads player stats from a JSON file."""
    try:
        stats_file = f"player_{player_id}_{match_type}_stats.json"
        stats_path = os.path.join(RAW_DIR, stats_file)
        with open(stats_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Player stats file not found for player {player_id} and match type {match_type}")
        return None
    except Exception as e:
        logging.error(f"Error loading player stats: {e}")
        return None

def load_player_profile(player_id):
    """Loads player profile from a parquet file."""
    try:
        profile_path = os.path.join(DATA_DIR, f"player_{player_id}_profile.parquet")
        return pd.read_parquet(profile_path)
    except FileNotFoundError:
        logging.error(f"Player profile file not found for player {player_id}")
        return None
    except Exception as e:
        logging.error(f"Error loading player profile: {e}")
        return None
