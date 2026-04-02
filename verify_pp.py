import sys
import os

# Add project root to sys.path
sys.path.append(os.getcwd())

from scrapers.prizepicks import scrape_prizepicks
import logging

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO)

def test():
    print("Starting PrizePicks Scrape Test...")
    # Only scrape NBA for testing
    test_leagues = {"NBA": True}
    lines = scrape_prizepicks(active_leagues=test_leagues)
    
    if lines:
        print(f"SUCCESS: Fetched {len(lines)} lines from PrizePicks.")
        print("Sample Line:")
        sample = lines[0]
        print(f"  Player: {sample.player_name}")
        print(f"  Stat: {sample.stat_type}")
        print(f"  Score: {sample.line_score}")
        print(f"  Side: {sample.side}")
    else:
        print("FAILURE: No lines fetched from PrizePicks.")

if __name__ == "__main__":
    test()
