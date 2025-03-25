import pandas as pd
import os
import logging
import json
import re
from datetime import datetime

DATA_DIR = "data/processed"
RAW_DIR = "data/raw"

logging.basicConfig(
    filename="utr_score.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

import json
import os
import re
from datetime import datetime

RAW_DIR = "data/raw"

def get_match_utr(player_id, match_data, match_type):
    """Retrieves the player's UTR for a specific match."""
    try:
        if match_type == "singles":
            # singles logic
            stats_file = f"player_{player_id}_{match_type}_stats.json"
            stats_path = os.path.join(RAW_DIR, stats_file)
            with open(stats_path, "r") as f:
                stats = json.load(f)
            trend_chart = stats.get("ratingTrendChart", {})
            if not trend_chart or not trend_chart.get("months"):
                return None
            match_date_str = match_data['descriptions'][0]['resultDate'].split('T')[0]
            match_date_obj = datetime.strptime(match_date_str, "%Y-%m-%d").date()
            for month in trend_chart["months"]:
                if month.get("results"):
                    for result in month['results']:
                        result_date = datetime.strptime(result['descriptions'][0]['resultDate'].split('T')[0], "%Y-%m-%d").date()
                        if result_date == match_date_obj:
                            for rating in month['ratings']:
                                return rating['ratingDisplay']
            return None
        else:  # doubles
            # doubles logic from json file
            stats_file = f"player_{player_id}_{match_type}_stats.json"
            stats_path = os.path.join(RAW_DIR, stats_file)
            with open(stats_path, "r") as f:
                stats = json.load(f)
            trend_chart = stats.get("ratingTrendChart", {})
            if not trend_chart or not trend_chart.get("months"):
                return None
            match_date_str = match_data['descriptions'][0]['resultDate'].split('T')[0]
            match_date_obj = datetime.strptime(match_date_str, "%Y-%m-%d").date()
            for month in trend_chart['months']:
                if month.get('results'):
                    for result in month['results']:
                        details = result['descriptions'][0]['details']
                        date_match = re.search(r"^(Sun|Mon|Tue|Wed|Thu|Fri|Sat)\s([A-Za-z]{3})\s(\d{1,2})", details)
                        utr_matches = re.findall(r"\((\d+\.\d+)/(\d+\.\d+)\)", details)
                        name_matches = re.findall(r"([A-Z]\.[A-Za-z]+)", details)

                        if date_match and utr_matches and name_matches:
                            day_of_week, month_abbr, day = date_match.groups()
                            year = datetime.now().year
                            if datetime.strptime(month_abbr, '%b').month > datetime.now().month:
                                year -= 1
                            row_date_str = f"{year}-{datetime.strptime(month_abbr, '%b').month:02d}-{int(day):02d}"
                            row_date_obj = datetime.strptime(row_date_str, "%Y-%m-%d").date()
                            if row_date_obj == match_date_obj:
                                if player_id in [player_id_lookup(player_id, name_matches[0]), player_id_lookup(player_id, name_matches[1])]:
                                    if player_id == player_id_lookup(player_id, name_matches[0]):
                                        return utr_matches[0][0]
                                    else:
                                        return utr_matches[0][1]
                                elif player_id in [player_id_lookup(player_id, name_matches[2]), player_id_lookup(player_id, name_matches[3])]:
                                    if player_id == player_id_lookup(player_id, name_matches[2]):
                                        return utr_matches[1][0]
                                    else:
                                        return utr_matches[1][1]
                                else:
                                    return None
                        else:
                            return None
            return None
    except Exception as e:
        print(f"Error retrieving match UTR: {e}")
        return None

def player_id_lookup(player_id, player_name):
    """Looks up the player ID based on the player name."""
    try:
        # 1. Load player profile
        profile_path = os.path.join(DATA_DIR, f"player_{player_id}_profile.parquet")
        player_profile = pd.read_parquet(profile_path)
        # 2. Extract player name
        first_name = player_profile['firstName'][0]
        last_name = player_profile['lastName'][0]
        formatted_name = f"{first_name[0]}.{last_name}"
        #3. compare player names.
        if formatted_name == player_name:
            return player_id
        else:
            return None
    except Exception as e:
        print(f"Error looking up player ID: {e}")
        return None

def calculate_player_utr(player_id):
    """Calculates and returns the singles and doubles UTR scores for a player."""
    try:
        # Load player data
        results_path = os.path.join(DATA_DIR, f"player_{player_id}_results.parquet")
        results_df = pd.read_parquet(results_path)

        match_utrs = {}

        for index, row in results_df.iterrows():
            match_id = row['event_id']  # correct column name.
            players_json_str = row['players']  # correct column name.
            match_data = json.loads(players_json_str)
            if match_data["winner2"] is None:
                match_type = "singles"
            else:
                match_type = "doubles"

            date = row['date']
            event_name = row['event_name']
            utr = get_match_utr(player_id, {"descriptions": [{"resultDate": date, "details": event_name}]}, match_type)
            if utr is not None:
                match_utrs[match_id] = {"utr": utr, "date": date}

        logging.info(f"Match UTRs calculated for player {player_id}: {match_utrs}")
        return match_utrs

    except Exception as e:
        logging.error(f"Error calculating UTR for player {player_id}: {e}")
        return None

# Process Michael
# calculate_player_utr(4140765)

# Process Christy
# calculate_player_utr(4313439)

