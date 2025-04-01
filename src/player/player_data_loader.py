import sqlite3
import os
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

DB_DIR = "data/db"
PLAYERS_DB_FILE = os.path.join(DB_DIR, "players.db")

def load_player_profile_from_db(player_id: str) -> dict | None:
    """Loads a player's profile from the database by their ID."""
    conn = sqlite3.connect(PLAYERS_DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM players WHERE id=?", (player_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        # Assuming the order of columns in the SELECT matches the table definition
        profile = {
            "id": row[0],
            "firstName": row[1],
            "lastName": row[2],
            "gender": row[3],
            "birthDate": row[4],
            "ageRange": row[5],
            "displayName": row[6],
            "myUtrSingles": row[7],
            "myUtrDoubles": row[8],
            "descriptionShort": row[9]
        }
        return profile
    return None

def load_multiple_player_profiles_from_db(player_ids: list[str]) -> dict[str, dict]:
    """Loads multiple player profiles from the database by their IDs."""
    profiles = {}
    for player_id in player_ids:
        profile = load_player_profile_from_db(player_id)
        if profile:
            profiles[player_id] = profile
    return profiles

def _create_matches_table():
    conn = sqlite3.connect(PLAYERS_DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            match_date TEXT,
            match_format TEXT,
            score TEXT,
            result TEXT,
            player1_id TEXT,
            player2_id TEXT,
            opponent1_id TEXT,
            opponent2_id TEXT,
            FOREIGN KEY (player1_id) REFERENCES players(id),
            FOREIGN KEY (player2_id) REFERENCES players(id),
            FOREIGN KEY (opponent1_id) REFERENCES players(id),
            FOREIGN KEY (opponent2_id) REFERENCES players(id)
        )
    """)
    conn.commit()
    conn.close()
    logger.info("Matches table created or already exists.")

def _insert_match(conn: sqlite3.Connection, match_data: dict):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO matches (
                match_id, match_date, match_format, score, result,
                player1_id, player2_id, opponent1_id, opponent2_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            match_data.get('match_id'),
            match_data.get('match_date'),
            match_data.get('match_format'),
            match_data.get('score'),
            match_data.get('result'),
            match_data.get('player1_id'),
            match_data.get('player2_id'),
            match_data.get('opponent1_id'),
            match_data.get('opponent2_id')
        ))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error inserting match {match_data.get('match_id')}: {e}")

def load_player_match_stats(player_id: str, file_path: str):
    """Loads a player's match statistics from a JSON file into the database."""
    _create_matches_table()
    if file_path.endswith(".json"):
        conn = sqlite3.connect(PLAYERS_DB_FILE)
        cursor = conn.cursor()
        try:
            with open(file_path, 'r') as f:
                player_results = json.load(f)
            logger.info(f"Loaded match stats for player '{player_id}' from JSON file: {file_path}")

            for event in player_results.get('events', []):
                for draw in event.get('draws', []):
                    match_format = "Singles" if draw.get('teamType') != "DOUBLES" else "Doubles"
                    for result in draw.get('results', []):
                        winner = result.get('winner')
                        loser = result.get('loser')
                        players = result.get('players')
                        match_id = result.get('id')
                        match_date_str = result.get('date')
                        match_date = match_date_str.split('T')[0] if match_date_str else None
                        score_parts = []
                        match_result = None
                        player1_id = None
                        player2_id = None
                        opponent1_id = None
                        opponent2_id = None

                        if not players or not winner or not loser or not match_id:
                            continue

                        # Extract score
                        score_data = result.get('score', {})
                        for set_num in sorted(score_data.keys()):
                            set_score = score_data[set_num]
                            score_parts.append(f"{set_score.get('winner', 0)}-{set_score.get('loser', 0)}")

                        if match_format == "Singles":
                            winner1_id = players.get('winner1', {}).get('id')
                            loser1_id = players.get('loser1', {}).get('id')

                            if winner1_id == player_id:
                                player1_id = player_id
                                opponent1_id = loser1_id
                                match_result = "Win"
                            elif loser1_id == player_id:
                                player1_id = player_id
                                opponent1_id = winner1_id
                                match_result = "Loss"

                        elif match_format == "Doubles":
                            winner_team_ids = {players.get('winner1', {}).get('id'), players.get('winner2', {}).get('id')}
                            loser_team_ids = {players.get('loser1', {}).get('id'), players.get('loser2', {}).get('id')}

                            if player_id in winner_team_ids:
                                match_result = "Win"
                                player1_id = player_id
                                # Try to identify partner and opponents
                                for win_player_id in winner_team_ids:
                                    if win_player_id != player_id:
                                        player2_id = win_player_id
                                        break
                                for lose_player_id in loser_team_ids:
                                    if opponent1_id is None:
                                        opponent1_id = lose_player_id
                                    elif opponent1_id != lose_player_id:
                                        opponent2_id = lose_player_id
                                        break
                            elif player_id in loser_team_ids:
                                match_result = "Loss"
                                player1_id = player_id
                                # Try to identify partner and opponents
                                for lose_player_id in loser_team_ids:
                                    if lose_player_id != player_id:
                                        player2_id = lose_player_id
                                        break
                                for win_player_id in winner_team_ids:
                                    if opponent1_id is None:
                                        opponent1_id = win_player_id
                                    elif opponent1_id != win_player_id:
                                        opponent2_id = win_player_id
                                        break

                        if player1_id and match_result:
                            match_data = {
                                'match_id': str(match_id),
                                'match_date': match_date,
                                'match_format': match_format,
                                'score': ", ".join(score_parts),
                                'result': match_result,
                                'player1_id': player1_id,
                                'player2_id': player2_id,
                                'opponent1_id': opponent1_id,
                                'opponent2_id': opponent2_id
                            }
                            _insert_match(conn, match_data)

        except FileNotFoundError:
            logger.error(f"Match stats JSON file not found: {file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON file {file_path}: {e}")
        except Exception as e:
            logger.error(f"Error loading match stats from JSON file {file_path}: {e}")
        finally:
            conn.close()
    elif file_path.endswith(".parquet"):
        logger.warning("Loading match stats from Parquet files is not yet implemented.")
    else:
        logger.warning(f"Unsupported file format for match stats: {file_path}")
