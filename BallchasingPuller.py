
import ballchasing as bc
from collections import OrderedDict
import pandas as pd
import os
from os.path import exists
from ratelimiter import RateLimiter
import regex as re
import uuid
from datetime import datetime

def use_regex(input_text):
    pattern = re.compile(r"[a-zA-Z]+", re.IGNORECASE)
    return pattern.match(input_text).group(0)

def get_players_data(replay, team):

    # Extract the id from the groups array
    group_id = replay.get("groups", [{}])[0].get("id", None)

    # and configures it into a single data frame line
    dict = replay[replay[team]['color']]['players']
    temp = pd.DataFrame.from_dict(dict)

    try:
        # Team Handling
        if replay[team]['name'] != '':
            team_name = replay[team]['name']
        else:
            team_name = ''
    except KeyError:
        # Handle the KeyError
        team_name = ''

    # Account handling
    platform = []
    platform_id = []
    for account in temp['id']:
        platform.append(account.get('platform'))
        platform_id.append(account.get('id'))
        
    temp['platform'] = platform
    temp['platform_id']= platform_id
    temp['group_id'] = group_id
    temp['key'] = 0

    ser_match = pd.Series(get_match_data(replay))
    df_match = ser_match.to_frame()
    df_match = df_match.transpose()
    df_match['team_name'] = team_name

    df_meta = temp[[
        'start_time',
        'end_time',
        'platform',
        'platform_id',
        'name',
        'car_id',
        'car_name',
        'group_id',
        'key'
        ]]

    df_meta = df_meta.join(df_match, on='key', how='left')
    df_meta = df_meta[[
        'group_id',
        'id',
        'created',
        'title',
        'match_type',
        'team_size',
        'duration',
        'overtime',
        'season',
        'season_type',
        'date',
        'start_time',
        'end_time',
        'platform',
        'platform_id',
        'name',
        'car_id',
        'car_name',
        'team_name'
    ]]

    # # Add in opponent name for aggregation later
    # print (df_meta)
    # if team_name != '':
    #     if team == 'blue':
    #         df_meta['opponent'] = replay['orange']['name']
    #     else:
    #         df_meta['opponent'] = replay['blue']['name']
    # else:
    #     df_meta['opponent'] = ''

    df_stats = pd.DataFrame.from_dict(temp['stats'])
    dict_stats = df_stats['stats'].to_dict()
    df_player_data = pd.DataFrame.from_dict(dict_stats)
    df_player_data = df_player_data.transpose()


    # Grab the individual player stats and attach them to the single row
    # to get the single row on every one of the player stats rows
    core = []
    for s in df_player_data['core']:
        core.append(s)
        dfcombined = pd.DataFrame(core)


    boost = []
    for s in df_player_data['boost']:
        boost.append(s)
        dfboost = pd.DataFrame(boost)


    movement = []
    for s in df_player_data['movement']:
        movement.append(s)
        dfmove = pd.DataFrame(movement)


    positioning = []
    for s in df_player_data['positioning']:
        if s.get('goals_against_while_last_defender') is None:
            s.update(goals_against_while_last_defender=0)
            s = OrderedDict(sorted(s.items()))
            positioning.append(s)
        else:
            s = OrderedDict(sorted(s.items()))
            positioning.append(s)
        dfpos = pd.DataFrame(positioning)
    demo = []
    for s in df_player_data['demo']:
        demo.append(s)
        dfdemo = pd.DataFrame(demo)

    dfcombined = dfcombined.join(dfboost, how='left')
    dfcombined = dfcombined.join(dfmove, how='left')
    dfcombined = dfcombined.join(dfpos, how='left')
    dfcombined = dfcombined.join(dfdemo, how='left')

    df_meta['uid'] = [uuid.uuid4() for _ in range(len(df_meta))]
    
    df_meta = df_meta[[
        'group_id',
        'uid',
        'id',
        'created',
        'title',
        'match_type',
        'team_size',
        'duration',
        'overtime',
        'season',
        'season_type',
        'date',
        'start_time',
        'end_time',
        'platform',
        'platform_id',
        'name',
        'car_id',
        'car_name',
        'team_name'
    ]]

    df_final = df_meta.join(dfcombined, how='left')
    df_final = df_final[df_final['score']>2]
     # Add match outcomes to data rows
    if df_final['goals'].sum() > df_final['goals_against'].sum()/3:
        df_final['game_outcome'] = 'W'
    else:
        df_final['game_outcome'] = 'L'

    return df_final

def get_match_data(replay):
    # Gets the relavant meta data for each match as a dictionary
    fields = [
        'id',
        'created',
        'title',
        'match_type',
        'team_size',
        'duration',
        'overtime',
        'season',
        'season_type',
        'date',
    ]
    metastep = dict((k, replay[k]) for k in (fields))
    return metastep

def process_individual_replays(replays):
    print()
    print()
    print('Processing Individual Replays:' )
    print()
    count = 0

    df_players = pd.DataFrame()

    for replay in replays:
        with rate_limiter:  # rate limit is 2 calls / second, 1000 calls an hour
            count = count + 1
            print(replay+ '(' + str(count) + ' / ' + str(len(replays)) + ')')
            try:
                replay_obj = api.get_replay(replay)
            except:
                print("Replay not found: ", replay)
                continue

            df_blue = get_players_data(replay_obj, 'blue')
            df_orange = get_players_data(replay_obj, 'orange')
            df_players =  pd.concat([df_blue, df_orange], ignore_index=True)
    
    return df_players

def process_group_replays(groups):
    print()
    print()
    print('Processing Groups:')
    print(groups)
    df_all_groups = pd.DataFrame()
    count = 0

    # Group in groups handles multiple group links in input file
    for group in groups:
        count = count+1
        print()
        print(group+' '+'('+str(count)+'/'+str(len(groups))+')')
        res = api.get_group_replays(
        group_id=group,
        deep=True
        )

        for replay in res:
            df_blue = get_players_data(replay, 'blue')
            df_orange = get_players_data(replay, 'orange')
            df_players = pd.concat([df_blue, df_orange], ignore_index=True)
            print(df_players.iloc[0,0])
            df_all_groups = pd.concat([df_all_groups,df_players])


    return df_all_groups

def replayStructure(replayType,Links):
    missed = []
    replays = []
    link_split = []

    count = 0
    for link in Links:
        count = count + 1
        if link == '':
            continue
        if link[:24] != "https://ballchasing.com/":
            continue
        
        link_split = link.split(sep='/')

        if link_split[3] == replayType:
            if count == len(Links):
                replays.append(link_split[4])
            else:
                replays.append(link_split[4][:-1])
        else:
            if count == len(Links)+1:
                missed.append(link_split[4])
            else:
                missed.append(link_split[4][:-1])

    if (len(missed) > 0): 
        print(missed)

    return replays


if __name__ == "__main__":

    with open('C:/Users/conno/Documents/Coding/GCBLeague/api_key.txt',"r") as f:
        apikey = f.readline()

    api = bc.Api(apikey)
    rate_limiter = RateLimiter(max_calls=4, period=1) #Patreon 

    inpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/input.txt'
    outpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/seasonData.csv'
    if exists(outpath):
        os.remove(outpath)

    f = open(inpath, "r")
    Links = f.readlines()
    # Read each line and strip the ball chasing match identifier from each
    replays = []
    groups = []

    replays = replayStructure("replay",Links)
    groups = replayStructure("group",Links)

    # Create an empty DataFrame to store all the data
    final_df = pd.DataFrame()
    final_df = pd.concat([final_df,process_individual_replays(replays)], ignore_index=True)
    final_df = pd.concat([final_df,process_group_replays(groups)], ignore_index=True)
    final_df['ETL'] = datetime.now()

    if exists(outpath):
        final_df.to_csv(outpath, mode='a', index=False, header=False)
    else:
        final_df.to_csv(outpath, index=False)
