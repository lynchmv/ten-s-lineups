import sqlite3
import os
from src.team.team import Team
from src.player.player_data_loader import load_player_profile_from_db

DB_DIR = "data/db"

class TeamManager:
    def __init__(self, db_file: str = os.path.join(DB_DIR, 'teams.db')):
        os.makedirs(DB_DIR, exist_ok=True)  # Ensure the directory exists
        self.db_file = db_file
        self._create_tables()

    def _create_tables(self):
        """Creates the teams and team_players tables if they don't exist."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                team_number TEXT PRIMARY KEY,
                year INTEGER,
                league_type TEXT,
                section TEXT,
                district TEXT,
                area TEXT,
                season TEXT,
                flight TEXT,
                name TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS team_players (
                team_number TEXT,
                player_id TEXT,
                FOREIGN KEY (team_number) REFERENCES teams(team_number),
                PRIMARY KEY (team_number, player_id)
            )
        """)
        conn.commit()
        conn.close()

    def create_team(self, team_number: str, year: int, league_type: str, section: str,
                    district: str, area: str, season: str, flight: str, name: str) -> Team:
        """Creates a new Team object and adds its basic info to the database."""
        team = Team(team_number, year, league_type, section, district, area, season, flight, name)
        self._add_team_basic_info_to_db(team)
        return team

    def _add_team_basic_info_to_db(self, team: Team):
        """Adds a Team object's basic data to the teams table."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO teams (team_number, year, league_type, section, district, area, season, flight, name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (team.team_number, team.year, team.league_type, team.section, team.district,
                  team.area, team.season, team.flight, team.name))
            conn.commit()
        except sqlite3.IntegrityError:
            print(f"Warning: Team with number '{team.team_number}' already exists.")
        finally:
            conn.close()

    def get_team(self, team_number: str) -> Team | None:
        """Retrieves a Team object from the database by its team number, including players."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Retrieve basic team info
        cursor.execute("SELECT * FROM teams WHERE team_number=?", (team_number,))
        team_row = cursor.fetchone()

        if team_row:
            team = Team(*team_row)
            # Retrieve players for this team
            cursor.execute("SELECT player_id FROM team_players WHERE team_number=?", (team_number,))
            player_rows = cursor.fetchall()
            for row in player_rows:
                team.add_player(row[0])
            conn.close()
            return team

        conn.close()
        return None

    def add_player_to_team(self, team_number: str, player_id: str):
        """Adds a player to a team's roster in the database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO team_players (team_number, player_id)
                VALUES (?, ?)
            """, (team_number, player_id))
            conn.commit()
        except sqlite3.IntegrityError:
            print(f"Warning: Player '{player_id}' is already on team '{team_number}'.")
        finally:
            conn.close()
            # Update the in-memory Team object if it exists
            team = self.get_team(team_number)
            if team and player_id not in team.players:
                team.add_player(player_id)

    def remove_player_from_team(self, team_number: str, player_id: str):
        """Removes a player from a team's roster in the database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM team_players WHERE team_number=? AND player_id=?", (team_number, player_id))
        conn.commit()
        conn.close()
        # Update the in-memory Team object if it exists
        team = self.get_team(team_number)
        if team and player_id in team.players:
            team.remove_player(player_id)

    def populate_roster(self, team: Team, player_ids: list[str]):
        """Loads player IDs and attempts to fetch their profiles from the database."""
        for player_id in player_ids:
            profile = load_player_profile_from_db(player_id)
            if profile:
                if player_id not in team.players:
                    team.add_player(player_id)
                print(f"Loaded profile for player '{player_id}' for team '{team.name}' from database.")
            else:
                print(f"Warning: No profile found in database for player ID '{player_id}'.")
