import pandas as pd
import os
import logging
import json

DATA_DIR = "data/processed"
RAW_DIR = "data/raw"

logging.basicConfig(
    filename="utr_score.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def calculate_weighted_average(ratings):
    """Calculates a weighted average of UTR ratings."""
    if not ratings:
        return None
    weights = [0.95 ** i for i in range(len(ratings))] # Exponential decay.
    weighted_sum = sum(r * w for r, w in zip(ratings, weights))
    return round(weighted_sum / sum(weights), 2)

def calculate_player_utr(player_id):
    """Calculates and returns the singles and doubles UTR scores for a player."""
    try:
        # Load player data
        singles_stats_path = os.path.join(RAW_DIR, f"player_{player_id}_singles_stats.json")
        doubles_stats_path = os.path.join(RAW_DIR, f"player_{player_id}_doubles_stats.json")

        with open(singles_stats_path, "r") as f:
            singles_stats = json.load(f)
        with open(doubles_stats_path, "r") as f:
            doubles_stats = json.load(f)

        # Extract ratingTrendChart data
        singles_trend = singles_stats.get("ratingTrendChart", {})
        doubles_trend = doubles_stats.get("ratingTrendChart", {})

        # Calculate weighted average of singles UTR
        singles_ratings = []
        if singles_trend and singles_trend.get("months"):
            for month in singles_trend["months"]:
                if month.get("ratings"):
                    for rating in month["ratings"]:
                        singles_ratings.append(float(rating["ratingDisplay"]))

        # Calculate weighted average of doubles UTR
        doubles_ratings = []
        if doubles_trend and doubles_trend.get("months"):
            for month in doubles_trend["months"]:
                if month.get("ratings"):
                    for rating in month["ratings"]:
                        doubles_ratings.append(float(rating["ratingDisplay"]))

        # Basic weighted average (can be refined)
        singles_utr = calculate_weighted_average(singles_ratings)
        doubles_utr = calculate_weighted_average(doubles_ratings)

        if singles_utr is not None:
            logging.info(f"Singles UTR calculated for player {player_id}: {singles_utr}")
        else:
            logging.warning(f"Could not calculate Singles UTR for player {player_id}.")

        if doubles_utr is not None:
            logging.info(f"Doubles UTR calculated for player {player_id}: {doubles_utr}")
        else:
            logging.warning(f"Could not calculate Doubles UTR for player {player_id}.")

        return {"singles_utr": singles_utr, "doubles_utr": doubles_utr}

    except Exception as e:
        logging.error(f"Error calculating UTR for player {player_id}: {e}")
        return {"singles_utr": None, "doubles_utr": None}

# Process Michael
# calculate_player_utr(4140765)

# Process Christy
# calculate_player_utr(4313439)

