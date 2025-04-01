# src/team/team.py

class Team:
    def __init__(self, team_number: str, year: int, league_type: str, section: str,
                 district: str, area: str, season: str, flight: str, name: str):
        self.team_number = team_number
        self.year = year
        self.league_type = league_type
        self.section = section
        self.district = district
        self.area = area
        self.season = season
        self.flight = flight
        self.name = name
        self.players = []  # Initialize an empty list to hold player IDs

    def add_player(self, player_id: str):
        """Adds a player ID to the team's roster."""
        if player_id not in self.players:
            self.players.append(player_id)

    def remove_player(self, player_id: str):
        """Removes a player ID from the team's roster."""
        if player_id in self.players:
            self.players.remove(player_id)

    def get_roster(self) -> list[str]:
        """Returns the current list of player IDs in the roster."""
        return self.players

    def __str__(self):
        return f"Team Name: {self.name} ({self.year} {self.league_type} - Flight {self.flight})"

    # We can add methods for saving and loading later
