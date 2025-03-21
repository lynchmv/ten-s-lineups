import argparse
from api.utr_api import UTRAPI
from processing.data_saver import save_player_profile, save_player_results, save_player_stats

def main():
    parser = argparse.ArgumentParser(description="UTR Player Lookup CLI")
    parser.add_argument("--player", "-p", help="Player name to search")
    args = parser.parse_args()

    api = UTRAPI()

    # Login
    if not api.login():
        print("Failed to log in. Exiting.")
        return

    while True:  # Loop to allow multiple searches
        if args.player:
            player_name = args.player
            args.player = None  # Clear the argument for subsequent loops
        else:
            player_name = input("\nEnter player name to search (or 'q' to quit): ").strip()

        if player_name.lower() == 'q':
            break  # Exit the loop if the user enters 'q'

        # Search for player
        player = api.search_player(player_name)
        if not player:
            print("No player selected. Try again.")
            continue  # Go to the next iteration of the loop

        # Fetch player profile
        print(f"\nFetching profile for {player['displayName']}...")
        profile = api.get_player_profile(player['id'])
        if profile:
            save_player_profile(profile, player["id"])
            print(f"Profile for {player['displayName']} retrieved successfully!")

        # Fetch player match results
        print(f"\nFetching results for {player['displayName']}...")
        results = api.get_player_results(player['id'])
        if results:
            save_player_results(results, player["id"])
            print(f"Match results retrieved for {player['displayName']}.")

        # Fetch player stats
        print(f"\nFetching stats for {player['displayName']}...")
        stats = api.get_player_stats(player['id'])
        if stats:
            save_player_stats(stats, player["id"])
            print(f"Match stats retrieved for {player['displayName']}.")

        # Display results summary
        print("\nSummary:")
        print(f"Name: {player['displayName']}")
        print(f"Location: {player['location']}")
        print(f"Total Matches: {len(results.get('events', [])) if results else 0}")
        print("-" * 40)

if __name__ == "__main__":
    main()
