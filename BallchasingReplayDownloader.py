import ballchasing as bc
import os
from os.path import exists
from ratelimiter import RateLimiter
import regex as re
from datetime import datetime

def use_regex(input_text):
    pattern = re.compile(r"[a-zA-Z]+", re.IGNORECASE)
    return pattern.match(input_text).group(0)

def process_individual_replays(replays, save_folder):
    print()
    print('Processing Individual Replays:')
    print()
    count = 0

    for replay in replays:
        with rate_limiter:  # rate limit
            count += 1
            print(f"{replay} ({count}/{len(replays)})")
            try:
                save_path = os.path.join(save_folder, f"{replay}.replay")
                os.makedirs(save_folder, exist_ok=True)
                with open(save_path, 'wb') as f:
                    f.write(api.download_replay_file(replay))
                print(f"Saved to {save_path}")
            except Exception as e:
                print(f"Replay not found or failed to download: {replay} | Error: {e}")

import requests

def process_group_replays(groups, save_folder):
    print()
    print('Processing Groups:')
    print(groups)
    count = 0

    for group in groups:
        count += 1
        print()
        print(group + ' ' + '(' + str(count) + '/' + str(len(groups)) + ')')
        
        try:
            with rate_limiter:
                res = list(api.get_group_replays(
                    group_id=group,
                    deep=True
                ))
        except Exception as e:
            print(f"Failed to fetch group {group}: {e}")
            continue

        total_replays = len(res)
        print(f"Found {total_replays} replays under group {group}")

        replay_download_count = 0
        for replay in res:
            if replay.get("id"):
                replay_download_count += 1
                replay_id = replay["id"]
                print(f"Downloading Replay {replay_download_count}/{total_replays}: {replay_id}")
                try:
                    save_path = os.path.join(save_folder, f"{replay_id}.replay")
                    os.makedirs(save_folder, exist_ok=True)
                    
                    response = requests.get(
                        f"https://ballchasing.com/api/replays/{replay_id}/file",
                        headers={"Authorization": apikey}
                    )

                    if response.status_code == 200:
                        with open(save_path, 'wb') as f:
                            f.write(response.content)
                        print(f"Saved to {save_path}")
                    else:
                        print(f"Failed to download {replay_id}: {response.status_code} - {response.text}")
                except Exception as e:
                    print(f"Failed to download {replay_id}: {e}")



def replayStructure(replayType, Links):
    missed = []
    replays = []
    link_split = []

    count = 0
    for link in Links:
        count += 1
        if link.strip() == '':
            continue
        if not link.startswith("https://ballchasing.com/"):
            continue
        
        link_split = link.strip().split(sep='/')

        if link_split[3] == replayType:
            replays.append(link_split[4].strip())
        else:
            missed.append(link_split[4].strip())

    if missed:
        print("Missed links (wrong type):", missed)

    return replays

if __name__ == "__main__":

    with open('C:/Users/conno/Documents/Coding/GCBLeague/api_key.txt', "r") as f:
        apikey = f.readline().strip()

    api = bc.Api(apikey)
    rate_limiter = RateLimiter(max_calls=4, period=1)  # Patreon

    inpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/input.txt'
    save_folder = 'C:/Users/conno/Documents/GrindhouseLeague/Season 3 Replays'

    with open(inpath, "r") as f:
        Links = f.readlines()

    # Read each line and strip the ballchasing identifiers
    replays = replayStructure("replay", Links)
    groups = replayStructure("group", Links)

    # Now process downloading
    if replays:
        process_individual_replays(replays, save_folder)
    else:
        print("No direct replays found.")

    if groups:
        process_group_replays(groups, save_folder)
    else:
        print("No groups found.")

    print("Download process completed at:", datetime.now())
