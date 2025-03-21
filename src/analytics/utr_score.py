import json
import pandas as pd

def load_json_data(masked_path, unmasked_path, player_id):
    with open(masked_path, "r") as f:
        masked_data = json.load(f)
    with open(unmasked_path, "r") as f:
        unmasked_data = json.load(f)
    return masked_data, unmasked_data

def extract_utr_shifts(data, masked=True, player_id=None):
    extracted = []
    for event in data.get("events", []):
        for draw in event.get("draws", []):
            for result in draw.get("results", []):
                opponent_utr = None
                for key, player in result.get("players", {}).items():
                    if player and str(player.get("id")) != str(player_id):
                        opponent_utr = player.get("myUtrSingles" if masked else "myUtrDoubles")
                for key, player in result.get("players", {}).items():
                    if player and str(player.get("id")) == str(player_id):
                        extracted.append({
                            "date": result.get("date"),
                            "event": event.get("id"),
                            "win": result.get("winner", {}).get("isWinner", "Unknown"),
                            "games_won": sum([set_data.get("winner", 0) for set_data in result.get("score", {}).values()]),
                            "games_lost": sum([set_data.get("loser", 0) for set_data in result.get("score", {}).values()]),
                            "utr_singles": player.get("myUtrSinglesDisplay" if masked else "myUtrSingles"),
                            "utr_doubles": player.get("myUtrDoublesDisplay" if masked else "myUtrDoubles"),
                            "opponent_utr": opponent_utr
                        })
    return pd.DataFrame(extracted)

def estimate_historical_singles_utr(df):
    df = df.sort_values("date", ascending=False)
    if not df.empty:
        df["estimated_singles_utr"] = df["utr_singles_unmasked"].astype(float)  # Use actual UTR for most recent
        for i in range(1, len(df)): # Start from the second row
            adjustment = 0.02 if df.iloc[i-1]["win_masked"] else -0.02
            df.at[i, "estimated_singles_utr"] = df.iloc[i-1]["estimated_singles_utr"] + adjustment
    return df

def estimate_historical_doubles_utr(df):
    df = df.sort_values("date", ascending=False)
    if not df.empty:
        df["estimated_doubles_utr"] = df["utr_doubles_unmasked"].astype(float) # Use actual UTR for most recent
        for i in range(1, len(df)): # Start from the second row
            adjustment = 0.02 if df.iloc[i-1]["win_masked"] else -0.02
            df.at[i, "estimated_doubles_utr"] = df.iloc[i-1]["estimated_doubles_utr"] + adjustment
    return df

def process_player(masked_path, unmasked_path, player_id, output_path):
    masked_data, unmasked_data = load_json_data(masked_path, unmasked_path, player_id)
    masked_df = extract_utr_shifts(masked_data, masked=True, player_id=player_id)
    unmasked_df = extract_utr_shifts(unmasked_data, masked=False, player_id=player_id)

    comparison_df = masked_df.merge(unmasked_df, on=["date", "event"], suffixes=("_masked", "_unmasked"))
    comparison_df = estimate_historical_singles_utr(comparison_df)
    comparison_df = estimate_historical_doubles_utr(comparison_df)

    comparison_df.to_csv(output_path, index=False)
    print(f"Processed UTR shifts for player {player_id}, saved to {output_path}")
    print(comparison_df.head())

# Process Michael
process_player("data/raw/michael_results.json", "data/raw/michael_unfiltered_results.json", 4140765, "data/utr_shift_michael.csv")

# Process Christy
process_player("data/raw/christy_results.json", "data/raw/christy_unfiltered_results.json", 4313439, "data/utr_shift_christy.csv")

