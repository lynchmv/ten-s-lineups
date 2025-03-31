import pandas as pd
import json
from datetime import datetime
from ..data_access import load_player_results, load_player_stats, load_player_profile
import os
import re
import logging

DATA_DIR = "data/processed"
RAW_DIR = "data/raw"

# Get a logger instance for this module
logger = logging.getLogger(__name__)

def player_id_lookup(player_id, player_name):
    """Looks up the player ID based on the player name."""
    logger.debug(f"Looking up player ID '{player_id}' for name '{player_name}'.")
    try:
        # 1. Load player profile
        player_profile = load_player_profile(player_id)
        if player_profile is None:
            logger.warning(f"Could not load profile for player ID '{player_id}' during lookup for name '{player_name}'.")
            return None
        # 2. Extract player name
        first_name = player_profile['firstName'][0]
        last_name = player_profile['lastName'][0]
        # 3. Compare player name with extracted name
        if player_name == f"{first_name[0]}.{last_name}":
            logger.debug(f"Found matching player ID '{player_id}' for name '{player_name}'.")
            return player_id
        else:
            logger.debug(f"Player name '{player_name}' does not match profile for ID '{player_id}'.")
            return None
    except Exception as e:
        logger.exception(f"An unexpected error occurred while looking up player ID '{player_id}' for name '{player_name}': {e}")
        return None

def get_match_utr(player_id, match_data, match_type):
    """Retrieves the player's UTR for a specific match."""
    logger.debug(f"Getting UTR for player '{player_id}', match: {match_data.get('descriptions')}, type: '{match_type}'.")
    try:
        if match_type == "singles":
            # singles logic
            logger.debug(f"Processing singles match for player '{player_id}'.")
            stats = load_player_stats(player_id, match_type)
            if stats is None:
                logger.warning(f"Could not load singles stats for player '{player_id}'.")
                return None
            trend_chart = stats.get("ratingTrendChart", {})
            if not trend_chart or not trend_chart.get("months"):
                logger.warning(f"Rating trend chart or months not found in singles stats for player '{player_id}'.")
                return None
            match_date_str = match_data['descriptions'][0]['resultDate'].split('T')[0]
            match_date_obj = datetime.strptime(match_date_str, "%Y-%m-%d").date()
            logger.debug(f"Looking for UTR on match date: {match_date_obj} for player '{player_id}'.")
            for month in trend_chart["months"]:
                if month.get("results"):
                    for result in month['results']:
                        result_date = datetime.strptime(result['descriptions'][0]['resultDate'].split('T')[0], "%Y-%m-%d").date()
                        if result_date == match_date_obj:
                            for rating in month['ratings']:
                                logger.debug(f"Found UTR {rating['ratingDisplay']} for player {player_id} on match date {match_date_obj}.")
                                return rating['ratingDisplay']
            logger.info(f"UTR not found for player '{player_id}' on match date {match_date_obj} (singles).")
            return None
        else:  # doubles
            # doubles logic from json file
            logger.debug(f"Processing doubles match for player '{player_id}'.")
            stats = load_player_stats(player_id, match_type)
            if stats is None:
                logger.warning(f"Could not load doubles stats for player '{player_id}'.")
                return None
            trend_chart = stats.get("ratingTrendChart", {})
            if not trend_chart or not trend_chart.get("months"):
                logger.warning(f"Rating trend chart or months not found in doubles stats for player '{player_id}'.")
                return None
            match_date_str = match_data['descriptions'][0]['resultDate'].split('T')[0]
            match_date_obj = datetime.strptime(match_date_str, "%Y-%m-%d").date()
            logger.debug(f"Looking for UTR on match date: {match_date_obj} (doubles) for player '{player_id}'.")
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
                                p1_name = name_matches[0]
                                p2_name = name_matches[1]
                                p3_name = name_matches[2]
                                p4_name = name_matches[3]
                                utr1 = utr_matches[0][0]
                                utr2 = utr_matches[0][1]
                                utr3 = utr_matches[1][0]
                                utr4 = utr_matches[1][1]
                                logger.debug(f"Found potential UTRs for match on {match_date_obj}: ({p1_name}: {utr1}/{p2_name}: {utr2}), ({p3_name}: {utr3}/{p4_name}: {utr4})")
                                if player_id in [player_id_lookup(player_id, name_matches[0]), player_id_lookup(player_id, name_matches[1])]:
                                    if player_id == player_id_lookup(player_id, name_matches[0]):
                                        logger.debug(f"Returning UTR '{utr1}' for player '{player_id}' (matched with '{p1_name}').")
                                        return utr_matches[0][0]
                                    else:
                                        logger.debug(f"Returning UTR '{utr2}' for player '{player_id}' (matched with '{p2_name}').")
                                        return utr_matches[0][1]
                                elif player_id in [player_id_lookup(player_id, name_matches[2]), player_id_lookup(player_id, name_matches[3])]:
                                    if player_id == player_id_lookup(player_id, name_matches[2]):
                                        logger.debug(f"Returning UTR '{utr3}' for player '{player_id}' (matched with '{p3_name}').")
                                        return utr_matches[1][0]
                                    else:
                                        logger.debug(f"Returning UTR '{utr4}' for player '{player_id}' (matched with '{p4_name}').")
                                        return utr_matches[1][1]
                                else:
                                    logger.debug(f"Player ID '{player_id}' not found among the players in the details: {details}")
                                    return None
                        else:
                            logger.warning(f"Skipped doubles match due to missing date/UTR/name information: {details}")
                            return None
            logger.info(f"UTR not found for player '{player_id}' on match date {match_date_obj} (doubles).")
            return None
    except Exception as e:
        logger.exception(f"An unexpected error occurred while retrieving match UTR for player '{player_id}': {e}")
        return None

def get_player_utr_scores(player_id):
    """Fetches and returns the singles and doubles UTR scores for a player."""
    logger.info(f"Fetching UTR scores for player '{player_id}'.")
    try:
        results_df = load_player_results(player_id)
        if results_df is None or results_df.empty:
            logger.warning(f"No results found for player '{player_id}', cannot calculate UTR.")
            return {}

        match_utrs = {}

        for index, row in results_df.iterrows():
            match_id = row['event_id']
            players_json_str = row['players']
            match_data = json.loads(players_json_str)
            if match_data["winner2"] is None:
                match_type = "singles"
            else:
                match_type = "doubles"
            logger.debug(f"Processing match ID '{match_id}' of type '{match_type}' for player '{player_id}'.")

            date = row['date']
            event_name = row['event_name']
            utr = get_match_utr(player_id, {"descriptions": [{"resultDate": date, "details": event_name}]}, match_type)
            if utr is not None:
                match_utrs[match_id] = {"utr": utr, "date": date}

        logger.info(f"Match UTRs calculated for player {player_id}")
        logger.debug(f"{match_utrs}")
        return match_utrs

    except Exception as e:
        logger.error(f"Error calculating UTR for player {player_id}: {e}")
        return None
