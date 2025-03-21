import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import re
import sys

# Load match results data
results_path = "data/processed/player_4140765_results.parquet"
df_results = pd.read_parquet(results_path)

# Use a regular expression to find the number between the underscores
match = re.search(r"player_(\d+)_results", results_path)

if match:
    player_id = int(match.group(1))
    print(f"====== Searching for player: {player_id} ======")
else:
    print("====== Player ID not found in the path. ====== ")

# Use 'name' from the base of each event
df_results["event_name"] = df_results["name"]

# Ensure 'draws' is a list, not a string
df_results["draws"] = df_results["draws"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

# Verify the conversion
print(df_results["draws"].apply(lambda x: type(x)).value_counts())  # Should show <class 'list'> only

# Function to clean event names
def clean_event_name(name):
    if not isinstance(name, str):
        return "Unknown"
    name = name.replace("\n", " ").replace("\r", " ")  # Remove newlines
    name = re.sub(r"\s+", " ", name)  # Replace multiple spaces with a single space
    return name.strip()

def extract_winner_info(draws):
    """Extracts 'isWinner' from 'draw' inside 'results'."""
    if isinstance(draws, list) and draws:
        for draw_entry in draws:
            if "results" in draw_entry and isinstance(draw_entry["results"], list):
                for result in draw_entry["results"]:
                    if "draw" in result and isinstance(result["draw"], dict):
                        is_winner = result["draw"].get("isWinner", "Unknown")
                        return is_winner
    return False

def extract_utr_rating(draws, player_id):
    """Extracts 'myUtrDoublesDisplay' or 'myUtrSinglesDisplay' from 'players' inside 'results'."""
    if isinstance(draws, list) and draws:
        for draw_entry in draws:
            if "results" in draw_entry and isinstance(draw_entry["results"], list):
                for result in draw_entry["results"]:
                    if "players" in result and isinstance(result["players"], dict):
                        # print(f"\nPlayers data: {json.dumps(result['players'], indent=2)}")  # Debugging

                        # Determine if it's singles or doubles based on "winner2"
                        match_type = "Doubles" if result["players"].get("winner2") and result["players"]["winner2"].get("id") else "Singles"
                        print(f"\n ---=== Match Type Detected: {match_type} ===---")

                        for key, player in result["players"].items():
                            if player is None:
                                continue

                            print(f"Checking player ID: {player.get('id')} against {player_id}")  # Debugging

                            if str(player.get("id")) == str(player_id):  # Ensure IDs match as strings
                                utr_value = player.get(
                                    "myUtrDoublesDisplay" if match_type == "Doubles" else "myUtrSinglesDisplay",
                                    "Unknown"
                                )
                                print(f"Found matching player! Returning UTR: {utr_value}")  # Debugging
                                return utr_value
    print("No matching player found.")
    return "Unknown"

# Apply cleaning function
df_results["event_name"] = df_results["event_name"].apply(clean_event_name)

# Determine win/loss status
df_results["win"] = df_results["draws"].apply(extract_winner_info)

# Extract match type (singles or doubles)
df_results["match_type"] = df_results.apply(
    lambda row: "Doubles" if row["draws"][0]["results"][0]["players"].get("winner2") else "Singles", axis=1
)

# Extract UTR rating
df_results["playerRating"] = df_results.apply(
    lambda row: extract_utr_rating(row["draws"], player_id), axis=1
)

# Print unique event names for verification
print(df_results["event_name"].value_counts().sort_values(ascending=False))

# Plot Wins vs Losses by Tournament (Singles)
plt.figure(figsize=(12, 6))
sns.countplot(y=df_results[df_results["match_type"] == "Singles"]["event_name"],
              hue=df_results[df_results["match_type"] == "Singles"]["win"],
              order=df_results["event_name"].value_counts().index)
plt.title("Win/Loss Distribution by Tournament (Singles)")
plt.xlabel("Number of Matches")
plt.ylabel("Tournament Name")
plt.legend(["Loss", "Win"], title="Match Result")
plt.show()

# Plot Wins vs Losses by Tournament (Doubles)
plt.figure(figsize=(12, 6))
sns.countplot(y=df_results[df_results["match_type"] == "Doubles"]["event_name"],
              hue=df_results[df_results["match_type"] == "Doubles"]["win"],
              order=df_results["event_name"].value_counts().index)
plt.title("Win/Loss Distribution by Tournament (Doubles)")
plt.xlabel("Number of Matches")
plt.ylabel("Tournament Name")
plt.legend(["Loss", "Win"], title="Match Result")
plt.show()

# Plot UTR Progress Over Time (Singles)
plt.figure(figsize=(10, 5))
sns.lineplot(x=pd.to_datetime(df_results[df_results["match_type"] == "Singles"]["startDate"]),
             y=df_results[df_results["match_type"] == "Singles"]["playerRating"],
             marker="o")
plt.xlabel("Date")
plt.ylabel("Player UTR")
plt.title("Player UTR Progress Over Time (Singles)")
plt.xticks(rotation=45)
plt.show()

# Plot UTR Progress Over Time (Doubles)
plt.figure(figsize=(10, 5))
sns.lineplot(x=pd.to_datetime(df_results[df_results["match_type"] == "Doubles"]["startDate"]),
             y=df_results[df_results["match_type"] == "Doubles"]["playerRating"],
             marker="o")
plt.xlabel("Date")
plt.ylabel("Player UTR")
plt.title("Player UTR Progress Over Time (Doubles)")
plt.xticks(rotation=45)
plt.show()
