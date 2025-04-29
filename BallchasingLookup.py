import ballchasing as bc
import pandas as pd
import os
from os.path import exists
from ratelimiter import RateLimiter
from datetime import datetime

def replayStructure(replayType, Links):
    missed = []
    replays = []

    for link in Links:
        link = link.strip()
        if link == '':
            continue
        if not link.startswith("https://ballchasing.com/"):
            continue
        
        link_split = link.split(sep='/')

        if link_split[3] == replayType:
            replays.append(link_split[4])
        else:
            missed.append(link_split[4])

    if missed:
        print("Missed links (wrong type):", missed)

    return replays

def extract_basic_player_info(replay_obj):
    """Extract group_id, id, created, platform_id, name from each replay"""
    records = []

    group_id = replay_obj.get("groups", [{}])[0].get("id", None)
    game_id = replay_obj.get("id")
    created = replay_obj.get("created")

    for team_color in ['blue', 'orange']:
        try:
            team_players = replay_obj[replay_obj[team_color]['color']]['players']
            for player in team_players:
                player_name = player.get('name')
                platform_id = player.get('id', {}).get('id')

                records.append({
                    'group_id': group_id,
                    'id': game_id,
                    'created': created,
                    'platform_id': platform_id,
                    'name': player_name
                })
        except Exception as e:
            print(f"Failed to extract player info for team {team_color}: {e}")

    return records

def process_individual_replays(replays):
    print("\nProcessing Individual Replays:")
    count = 0
    all_records = []

    for replay_id in replays:
        with rate_limiter:
            count += 1
            print(f"{replay_id} ({count}/{len(replays)})")
            try:
                replay_obj = api.get_replay(replay_id)
                records = extract_basic_player_info(replay_obj)
                all_records.extend(records)
            except Exception as e:
                print(f"Replay not found or failed: {replay_id} | Error: {e}")

    return pd.DataFrame(all_records)

def process_group_replays(groups):
    print("\nProcessing Groups:")
    print(groups)
    all_records = []
    count = 0

    for group_id in groups:
        count += 1
        print()
        print(f"{group_id} ({count}/{len(groups)})")

        try:
            with rate_limiter:
                res = list(api.get_group_replays(
                    group_id=group_id,
                    deep=True
                ))
        except Exception as e:
            print(f"Failed to fetch group {group_id}: {e}")
            continue

        print(f"Found {len(res)} replays under group {group_id}")

        for replay in res:
            if replay.get("id"):
                replay_id = replay["id"]
                with rate_limiter:
                    try:
                        replay_obj = api.get_replay(replay_id)
                        records = extract_basic_player_info(replay_obj)
                        all_records.extend(records)
                    except Exception as e:
                        print(f"Failed to fetch replay {replay_id}: {e}")

    return pd.DataFrame(all_records)

if __name__ == "__main__":

    with open('C:/Users/conno/Documents/Coding/GCBLeague/api_key.txt',"r") as f:
        apikey = f.readline().strip()

    api = bc.Api(apikey)
    rate_limiter = RateLimiter(max_calls=4, period=1)  # Patreon API limit

    inpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/input.txt'
    outpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/GameIndex.csv'

    if exists(outpath):
        os.remove(outpath)

    with open(inpath, "r") as f:
        Links = f.readlines()

    replays = replayStructure("replay", Links)
    groups = replayStructure("group", Links)

    final_df = pd.DataFrame()

    if replays:
        final_df = pd.concat([final_df, process_individual_replays(replays)], ignore_index=True)

    if groups:
        final_df = pd.concat([final_df, process_group_replays(groups)], ignore_index=True)

    if not final_df.empty:
        # Optional: sort nicely
        final_df['created'] = pd.to_datetime(final_df['created'])
        final_df = final_df.sort_values(by=['group_id', 'created', 'id'])

        print(final_df)

        if exists(outpath):
            final_df.to_csv(outpath, mode='a', index=False, header=False)
        else:
            final_df.to_csv(outpath, index=False)

    else:
        print("No data collected.")
