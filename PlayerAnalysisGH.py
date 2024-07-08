import pandas as pd
from os.path import exists
import pandasql as ps
from datetime import datetime
debug = 1

def create_matchhistory(df):
    df_games = df[['group_id',
        'id',
        'created',
        'title',
        'date',
        'Discord',
        'platform',
        'platform_id',
        'name',
        'Team_match',
        'game_outcome',
        'Team',
        'League',
        'uid',
        'match_type',
        'team_size',
        'duration',
        'overtime',
        'season',
        'season_type',
        'start_time',
        'end_time',
        'car_id',
        'car_name',
        'shots',
        'shots_against',
        'goals',
        'goals_against',
        'saves',
        'assists',
        'score',
        'mvp',
        'shooting_percentage',
        'bpm',
        'bcpm',
        'avg_amount',
        'amount_collected',
        'amount_stolen',
        'amount_collected_big',
        'amount_stolen_big',
        'amount_collected_small',
        'amount_stolen_small',
        'count_collected_big',
        'count_stolen_big',
        'count_collected_small',
        'count_stolen_small',
        'amount_overfill',
        'amount_overfill_stolen',
        'amount_used_while_supersonic',
        'time_zero_boost',
        'percent_zero_boost',
        'time_full_boost',
        'percent_full_boost',
        'time_boost_0_25',
        'time_boost_25_50',
        'time_boost_50_75',
        'time_boost_75_100',
        'percent_boost_0_25',
        'percent_boost_25_50',
        'percent_boost_50_75',
        'percent_boost_75_100',
        'avg_speed',
        'total_distance',
        'time_supersonic_speed',
        'time_boost_speed',
        'time_slow_speed',
        'time_ground',
        'time_low_air',
        'time_high_air',
        'time_powerslide',
        'count_powerslide',
        'avg_powerslide_duration',
        'avg_speed_percentage',
        'percent_slow_speed',
        'percent_boost_speed',
        'percent_supersonic_speed',
        'percent_ground',
        'percent_low_air',
        'percent_high_air',
        'avg_distance_to_ball',
        'avg_distance_to_ball_no_possession',
        'avg_distance_to_ball_possession',
        'avg_distance_to_mates',
        'goals_against_while_last_defender',
        'percent_behind_ball',
        'percent_closest_to_ball',
        'percent_defensive_half',
        'percent_defensive_third',
        'percent_farthest_from_ball',
        'percent_infront_ball',
        'percent_most_back',
        'percent_most_forward',
        'percent_neutral_third',
        'percent_offensive_half',
        'percent_offensive_third',
        'time_behind_ball',
        'time_closest_to_ball',
        'time_defensive_half',
        'time_defensive_third',
        'time_farthest_from_ball',
        'time_infront_ball',
        'time_most_back',
        'time_most_forward',
        'time_neutral_third',
        'time_offensive_half',
        'time_offensive_third',
        'inflicted',
        'taken'
        ]].copy()

    # df.to_csv('GCBLeague/GrindhouseProjects/initialresults.csv')
    # Convert 'created' to datetime and set timezone
    df_games['created'] = pd.to_datetime(df_games['created']).dt.tz_convert('UTC')
    for index, row in df_games.iterrows():
        if 'coaches' in row['group_id']:
            df_games.loc[index, 'League'] = 'Coaches'
    df_games.to_csv('GCBLeague/GrindhouseProjects/results.csv')
    query = """
        WITH RankedGames AS (
            SELECT DISTINCT
                df1.*,
                df2.Team as Team_opponent,
                '[' || CASE
                    WHEN df1.date < '2024-05-07' THEN 'Week 1'
                    WHEN df1.date < '2024-05-14' and df1.date > '2024-05-06' THEN 'Week 2'
                    WHEN df1.date < '2024-05-21' and df1.date > '2024-05-13' THEN 'Week 3'
                    WHEN df1.date < '2024-05-28' and df1.date > '2024-05-20' THEN 'Week 4'
                    WHEN df1.date < '2024-06-04' and df1.date > '2024-05-27' THEN 'Week 5'
                    WHEN df1.date < '2024-06-11' and df1.date > '2024-06-05' THEN 'Week 6'
                    WHEN df1.date < '2024-06-18' and df1.date > '2024-06-10' THEN 'Week 7'
                    WHEN df1.date < '2024-06-24' and df1.date > '2024-06-17' THEN 'Week 8'
                    WHEN df1.date < '2024-07-02' and df1.date > '2024-06-24' THEN 'Week 9'
                    WHEN df1.date < '2024-07-09' and df1.date > '2024-07-01' THEN 'Week 10'
                    ELSE ''
                END || ']' || df1.Team || ' vs ' || df2.Team || '(' || df1.League || ')' AS match_name
                FROM df_games df1
                LEFT JOIN df_games df2
                    ON df2.id = df1.id
                    AND df2.created = df1.created
                WHERE df1.Team <> df2.Team
        )

        SELECT DISTINCT 
            [group_id],
            [uid],
            [id],
            [created],
            [title],
            [match_type],
            [team_size],
            [duration],
            [overtime],
            [season],
            [season_type],
            [date],
            [start_time],
            [end_time],
            [Discord],
            [platform],
            [platform_id],
            [name],
            [car_id],
            [car_name],
            [Team],
            [Team_opponent],
            [game_outcome],
            [shots],
            [shots_against],
            [goals],
            [goals_against],
            [saves],
            [assists],
            [score],
            [mvp],
            [shooting_percentage],
            [bpm],
            [bcpm],
            [avg_amount],
            [amount_collected],
            [amount_stolen],
            [amount_collected_big],
            [amount_stolen_big],
            [amount_collected_small],
            [amount_stolen_small],
            [count_collected_big],
            [count_stolen_big],
            [count_collected_small],
            [count_stolen_small],
            [amount_overfill],
            [amount_overfill_stolen],
            [amount_used_while_supersonic],
            [time_zero_boost],
            [percent_zero_boost],
            [time_full_boost],
            [percent_full_boost],
            [time_boost_0_25],
            [time_boost_25_50],
            [time_boost_50_75],
            [time_boost_75_100],
            [percent_boost_0_25],
            [percent_boost_25_50],
            [percent_boost_50_75],
            [percent_boost_75_100],
            [avg_speed],
            [total_distance],
            [time_supersonic_speed],
            [time_boost_speed],
            [time_slow_speed],
            [time_ground],
            [time_low_air],
            [time_high_air],
            [time_powerslide],
            [count_powerslide],
            [avg_powerslide_duration],
            [avg_speed_percentage],
            [percent_slow_speed],
            [percent_boost_speed],
            [percent_supersonic_speed],
            [percent_ground],
            [percent_low_air],
            [percent_high_air],
            [avg_distance_to_ball],
            [avg_distance_to_ball_no_possession],
            [avg_distance_to_ball_possession],
            [avg_distance_to_mates],
            [goals_against_while_last_defender],
            [percent_behind_ball],
            [percent_closest_to_ball],
            [percent_defensive_half],
            [percent_defensive_third],
            [percent_farthest_from_ball],
            [percent_infront_ball],
            [percent_most_back],
            [percent_most_forward],
            [percent_neutral_third],
            [percent_offensive_half],
            [percent_offensive_third],
            [time_behind_ball],
            [time_closest_to_ball],
            [time_defensive_half],
            [time_defensive_third],
            [time_farthest_from_ball],
            [time_infront_ball],
            [time_most_back],
            [time_most_forward],
            [time_neutral_third],
            [time_offensive_half],
            [time_offensive_third],
            [inflicted],
            [taken],
            FIRST_VALUE(match_name) OVER (PARTITION BY group_id) AS match_name,
            DENSE_RANK() OVER (PARTITION BY group_id ORDER BY date) AS game_number
        FROM RankedGames
        ORDER BY date ASC
    """


    # Execute the SQL query
    df_with_opponent = ps.sqldf(query, locals())
    # df_with_opponent.to_csv('GCBLeague/GrindhouseProjects/AllMatches.csv')

        # Set game_mode based on match_name
    df_with_opponent['game_mode'] = '3v3'  # Default value

    for index, row in df_with_opponent.iterrows():
        if 'Coaches' in row['match_name'] == 2:
            df_with_opponent.at[index, 'game_mode'] = '2v2'
    
    df_with_opponent.to_csv('GCBLeague/GrindhouseProjects/AllMatches.csv')

    # Create a new DataFrame for game-level statistics
    game_level_stats = df_with_opponent.groupby(['match_name', 'game_number', 'Team','id'])['goals'].sum().reset_index()
    game_level_stats = game_level_stats.rename(columns={'goals': 'total_goals'})
    # game_level_stats.to_csv('GCBLeague/GrindhouseProjects/stepresults.csv')
    # Create a new DataFrame for opponent game-level statistics
    opponent_game_level_stats = df_with_opponent.groupby(['match_name', 'game_number', 'Team_opponent'])['goals'].sum().reset_index()
    opponent_game_level_stats = opponent_game_level_stats.rename(columns={'Team_opponent': 'Team'})
    opponent_game_level_stats = opponent_game_level_stats.rename(columns={'goals': 'total_goals_opponent'})

    # Merge the two DataFrames based on match_name, game_number, and Team
    game_level_stats = pd.merge(game_level_stats, opponent_game_level_stats, on=['match_name', 'game_number', 'Team'], how='outer')

    # Fill NaN values in team goals with 0
    game_level_stats['total_goals'].fillna(0, inplace=True)

    # Fill NaN values in opponent goals with 0
    game_level_stats['total_goals_opponent'].fillna(0, inplace=True)

    # Group by match_name, Team, and game_number to get aggregated statistics
    results_grouped = game_level_stats.groupby(['match_name', 'Team', 'game_number']).agg(
        team_goals=('total_goals', 'sum')
    ).reset_index()

    # Join results grouped back on itself by match name and game number
    joined_results_grouped = pd.merge(results_grouped, results_grouped, on=['match_name', 'game_number'], suffixes=('_team', '_opponent'))
    # joined_results_grouped.to_csv('GCBLeague/GrindhouseProjects/stepresults.csv')
    # Drop rows where team_team is equal to team_opponent
    joined_results_grouped = joined_results_grouped[joined_results_grouped['Team_team'] != joined_results_grouped['Team_opponent']]
    # joined_results_grouped.to_csv('GCBLeague/GrindhouseProjects/stepresults.csv')
    # Iterate through each match
    for match_name in joined_results_grouped['match_name'].unique():
        match_rows = joined_results_grouped[joined_results_grouped['match_name'] == match_name]
        first_occurrence = match_rows.iloc[0]

        # Filter the match_rows DataFrame to keep only rows where the first occurrence of Team_opponent is not equal to Team_team
        filtered_rows = match_rows[match_rows['Team_opponent'] != first_occurrence['Team_team']]

        # Update joined_results_grouped with the filtered rows for this match
        joined_results_grouped = joined_results_grouped[joined_results_grouped['match_name'] != match_name]
        joined_results_grouped = pd.concat([joined_results_grouped, filtered_rows])
    
    # Drop duplicates based on match_name and game_number (keeping the first occurrence)
    # joined_results_grouped = joined_results_grouped.drop_duplicates(subset=['match_name', 'game_number'])
    # joined_results_grouped.to_csv('GCBLeague/GrindhouseProjects/whatisthis.csv')
    
    # Create a new DataFrame for match history
    match_history = joined_results_grouped[['match_name', 'Team_team', 'team_goals_team', 'Team_opponent', 'team_goals_opponent','game_number']].copy()
    # match_history.to_csv('GCBLeague/GrindhouseProjects/results.csv')
    # Create columns for series record
    match_history['Series_Record_Team_team'] = 0
    match_history['Series_Record_Team_opponent'] = 0
    # match_history.to_csv('GCBLeague/GrindhouseProjects/results.csv')
    # Group by match_name
    grouped = match_history.groupby('match_name')
    # grouped.to_csv('GCBLeague/GrindhouseProjects/results.csv')
    # Iterate through each group
    for name, group in grouped:
        series_record_team_team = 0
        series_record_team_opponent = 0

        for index, row in group.iterrows():
            if row['team_goals_team'] > row['team_goals_opponent']:
                series_record_team_team += 1
            elif row['team_goals_team'] < row['team_goals_opponent']:
                series_record_team_opponent += 1

            match_history.at[index, 'Series_Record_Team_team'] = series_record_team_team
            match_history.at[index, 'Series_Record_Team_opponent'] = series_record_team_opponent

    # Combine series record columns into a string format
    match_history['Series_Record'] = match_history['Series_Record_Team_team'].astype(str) + '-' + match_history['Series_Record_Team_opponent'].astype(str)

    # Set game_mode based on match_name
    match_history['game_mode'] = '3v3'  # Default value

    # Iterate over rows to set game_mode
    for index, row in match_history.iterrows():
        if 'Coaches' in row['match_name']:
            match_history.at[index, 'game_mode'] = '2v2'

    # Get the overall goals scored
    match_history['Goals_Scored_Team_team'] = match_history.groupby('match_name')['team_goals_team'].transform('sum')
    match_history['Goals_Scored_Team_opponent'] = match_history.groupby('match_name')['team_goals_opponent'].transform('sum')
    match_history.to_csv('GCBLeague/GrindhouseProjects/full_match_history.csv')
    match_history['Points'] = None
    # match_history.to_csv('GCBLeague/GrindhouseProjects/results.csv')
    rSweepFlag = False
    for index,row in match_history.iterrows():
        game_mode = row['game_mode']
        Team1_Series = row['Series_Record_Team_team']
        Team2_Series = row['Series_Record_Team_opponent']
        Team1_goals = row['Goals_Scored_Team_team']
        Team2_goals = row['Goals_Scored_Team_opponent']

        if game_mode == '3v3':
            if (Team1_Series < 3 & Team2_Series < 3):
                continue
            elif(((Team1_Series == 3) & (Team2_Series == 0) & (Team2_goals == 0))|((Team2_Series == 3) & (Team1_Series == 0) & (Team1_goals == 0))):
                match_history.at[index, 'Points']= 10
            elif(((Team1_Series == 3) & (Team2_Series == 0))|((Team2_Series == 3) & (Team1_Series == 0))):
                match_history.at[index, 'Points'] = 8
            elif ((Team1_Series == 3)|(Team2_Series == 3)):
                match_history.at[index, 'Points'] = 6
            
        if game_mode == '2v2':
            if((Team1_Series == 2)|(Team2_Series == 2)):
                match_history.at[index, 'Points'] = 2
            else:
                continue
            if(((Team1_Series == 2) & (Team2_Series == 0))|((Team2_Series == 2) & (Team1_Series == 0))):
                match_history.at[index, 'Points'] = 4
            if(((Team1_Series == 2) & (Team2_goals == 0))|((Team2_Series == 2) & (Team1_goals == 0))):
                match_history.at[index, 'Points']= 6
        match_history.to_csv('GCBLeague/GrindhouseProjects/results.csv')
    
    match_history = match_history[match_history['Points']>0]
    match_history['Points'] = match_history['Points'].astype(int)
    # match_history.to_csv('GCBLeague/GrindhouseProjects/results.csv')

    final_match_history = match_history.drop(columns=['game_number','team_goals_team','team_goals_opponent'])
    # final_match_history.to_csv('GCBLeague/GrindhouseProjects/full_match_history.csv')
    return final_match_history

def create_leaderboard(df):
    all_teams_performance = pd.concat([df['Team_team'], df['Team_opponent']]).unique()
    all_teams_performance = pd.DataFrame({'Team': all_teams_performance, 'Team_Points': 0, 'Wins': 0, 'Losses': 0})

    # df.to_csv('GCBLeague/GrindhouseProjects/results.csv')

    all_teams_performance['Wins'] = 0
    all_teams_performance['Losses'] = 0

    # Iterate through each match in final match history
    for index, row in df.iterrows():
        Team1 = row['Team_team']
        Team1_s = row['Series_Record_Team_team']
        Team2 = row['Team_opponent']
        Team2_s = row['Series_Record_Team_opponent']
        points = row['Points']

        gm = row['game_mode']
        if gm == '3v3':
            if Team1_s == 3:  # Team_team won
                all_teams_performance.loc[all_teams_performance['Team'] == Team1, 'Wins'] += 1
                all_teams_performance.loc[all_teams_performance['Team'] == Team2, 'Losses'] += 1
                all_teams_performance.loc[all_teams_performance['Team'] == Team1, 'Team_Points'] += points
            if Team2_s == 3:  # Team_opponent won
                all_teams_performance.loc[all_teams_performance['Team'] == Team2, 'Wins'] += 1
                all_teams_performance.loc[all_teams_performance['Team'] == Team1, 'Losses'] += 1
                all_teams_performance.loc[all_teams_performance['Team'] == Team2, 'Team_Points'] += points
        if gm == '2v2':
            if Team1_s == 2:  # Team_team won
                all_teams_performance.loc[all_teams_performance['Team'] == row['Team_team'], 'Wins'] += 1
                all_teams_performance.loc[all_teams_performance['Team'] == row['Team_opponent'], 'Losses'] += 1
                all_teams_performance.loc[all_teams_performance['Team'] == row['Team_team'], 'Team_Points'] += points
            if Team2_s == 2:  # Team_opponent won
                all_teams_performance.loc[all_teams_performance['Team'] == row['Team_opponent'], 'Wins'] += 1
                all_teams_performance.loc[all_teams_performance['Team'] == row['Team_team'], 'Losses'] += 1
                all_teams_performance.loc[all_teams_performance['Team'] == row['Team_opponent'], 'Team_Points'] += points

    all_teams_performance['Series_Record'] = all_teams_performance['Wins'].astype(str) + '-' + all_teams_performance['Losses'].astype(str)
    all_teams_performance = all_teams_performance[['Team', 'Wins', 'Losses', 'Series_Record', 'Team_Points']]
    all_teams_performance = all_teams_performance.sort_values(by='Team_Points', ascending=False)

    # all_teams_performance.to_csv('GCBLeague/GrindhouseProjects/results.csv')
    return all_teams_performance


def merge_data(season_live_path, player_index_path):
    # Read the CSV files into DataFrames
    season_live = pd.read_csv(season_live_path)
    player_index = pd.read_csv(player_index_path)

    # Rename team_name column to Team in season_live DataFrame
    season_live = season_live.rename(columns={'team_name': 'Team_match'})
    get_names = """
    select s1.platform_id,
        s1.distinct_names,
        coalesce(pi1.Discord,pi2.Discord) AS Discord
    FROM (
        SELECT platform_id, 
        GROUP_CONCAT(DISTINCT name) AS distinct_names
        FROM season_live
        GROUP BY platform_id
    ) s1
    LEFT JOIN player_index pi1 
        ON s1.platform_id = pi1.platform_id
    LEFT JOIN player_index pi2
        ON s1.platform_id = pi2.alt_platform_id
    ORDER BY 3
    """
    all_players = ps.sqldf(get_names,locals())
    all_players.to_csv('GCBLeague/GrindhouseProjects/all_names.csv')

    # Define the SQL query for the merge
    query = """
    SELECT season_live.*, 
    coalesce(pi1.Team, pi2.Team) AS Team,
    coalesce(pi1.League, pi2.League) AS League,
    coalesce(pi1.Discord,pi2.Discord) AS Discord
    FROM season_live
    LEFT JOIN player_index pi1 
        ON season_live.platform_id = pi1.platform_id
    LEFT JOIN player_index pi2
        ON season_live.platform_id = pi2.alt_platform_id
    """
    merged_data = ps.sqldf(query, locals())
    merged_data.to_csv('GCBLeague/GrindhouseProjects/merged_data.csv', index=False)

    return merged_data

def insert_row(df,match_name,Team_team,Team_opponent,Series_Record_Team_team,Series_Record_Team_opponent,Series_Record,game_mode,Goals_Scored_Team_team,Goals_Scored_Team_opponent,Points):
    df.loc[-1] = [match_name,Team_team,Team_opponent,Series_Record_Team_team,Series_Record_Team_opponent,Series_Record,game_mode,Goals_Scored_Team_team,Goals_Scored_Team_opponent,Points]
    df.index = df.index + 1  # shifting index
    df = df.sort_index(ascending=True)
    return df

def admin_adjustments(match_history):
    match_history = insert_row(match_history,'[Week 1]FarmersOnly vs Team XV(S)- Admin Adjust XV', 'FarmersOnly','Team XV',1,3,1-3,'3v3',8,10,6)
    match_history = insert_row(match_history,'[Week 1]FarmersOnly vs Team XV(Coaches)- Admin Adjust FARM', 'FarmersOnly','Team XV',2,0,2-0,'2v2',0,0,4)
    match_history = insert_row(match_history,'[Week 3]FarmersOnly vs How Do You Like Your Eggs?(Coaches)- Admin Adjust FARM', 'FarmersOnly','How Do You Like Your Eggs?',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 4]High Stakes vs FarmersOnly(C)- RSweep Bonus HS', 'High Stakes','FarmersOnly',3,0,3-0,'3v3',0,0,1)
    match_history = insert_row(match_history,'[Week 5]High Stakes vs Team XV(C)- Admin Adjust HS', 'High Stakes','Team XV',3,0,3-0,'3v3',1,14,-2)
    match_history = insert_row(match_history,'[Week 5]The Cosmos vs How Do You Like Your Eggs?(Coaches)- Admin Adjust COS', 'The Cosmos','How Do You Like Your Eggs?',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 4]Funke Monke vs How Do You Like Your Eggs?(Coaches)- Admin Adjust FUNK', 'Funke Monke','How Do You Like Your Eggs?',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 6]The Cosmos vs Team XV(S)- Admin Adjust COS', 'The Cosmos','Team XV',3,2,3-2,'3v3',0,0,6)
    match_history = insert_row(match_history,'[Week 5]High Stakes vs Team XV(Coaches)- Admin Adjust HS', 'High Stakes','Team XV',2,1,2-1,'2v2',2,1,2)
    match_history = insert_row(match_history,'[Week 7]FarmersOnly vs The Cosmos(Coaches)- Admin Adjust FO', 'FarmersOnly','The Cosmos',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 8]FarmersOnly vs How Do You Like Your Eggs?(B)- Bad Upload Fix', 'FarmersOnly','How Do You Like Your Eggs?',0,3,0-3,'3v3',0,3,8)
    match_history = insert_row(match_history,'[Week 8]High Stakes vs The Cosmos(Coaches)- Admin Adjust COS', 'High Stakes','The Cosmos',0,2,0-2,'2v2',0,2,4)
    match_history = insert_row(match_history,'[Week 9]Funke Monke vs The Cosmos(C)- Admin Adjust COS', 'Funke Monke','The Cosmos',2,3,2-3,'3v3',2,3,6)
    match_history = insert_row(match_history,'[Week 9]FarmersOnly vs Team XV(Coaches)- Admin Adjust FARM', 'FarmersOnly','Team XV',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 9]High Stakes vs How Do You Like Your Eggs?(Coaches)- Admin Adjust HS', 'High Stakes','How Do You Like Your Eggs?',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 8]Funke Monke vs Team XV(Coaches)- Admin Adjust Funk', 'Funke Monke','Team XV',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 10]How Do You Like Your Eggs? vs Team XV(Coaches)- Admin Adjust EGG', 'How Do You Like Your Eggs?','Team XV',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 10]Funke Monke vs FarmersOnly(Coaches)- Admin Adjust COS', 'The Cosmos','FarmersOnly',2,0,2-0,'2v2',2,0,4)
    match_history = insert_row(match_history,'[Week 9]Funke Monke vs The Cosmos(B)- Admin Adjust COS', 'Funke Monke','The Cosmos',0,3,0-3,'3v3',2,3,8)
    match_history = insert_row(match_history,'[Week 8]Funke Monke vs The Cosmos(S)- RSweep Bonus FUNK', 'Funke Monke','The Cosmos',3,0,3-0,'3v3',0,0,1)
    match_history = insert_row(match_history,'[Week 9]How Do You Like Your Eggs? vs Team XV(Coaches)- Stream Miss Penalty EGG', 'How Do You Like Your Eggs?', 'High Stakes',3,0,3-0,'3v3',3,0,-2)
    return match_history

def player_superlatives(merged_data,player_index):
    player_index = pd.read_csv(player_index)
    query = '''
    with agg_step as (
        Select Distinct
        Discord,
        SUM(goals) AS Goals,
        SUM(saves) AS Saves,
        SUM(assists) AS Assists,
        SUM(shots) AS Shots,
        SUM(goals/shots)*100 AS [Shooting Percentage],
        SUM(inflicted) AS Inflicted
        FROM merged_data 
        GROUP BY Discord
        
    )
    
    SELECT pi.Discord as Player,
    pi.platform_id,
    pi.League,
    pi.Team,
    a.Goals,
    RANK() OVER (ORDER BY Goals DESC) AS [Goals Ranking],
    a.Saves,
    RANK() OVER (ORDER BY Saves DESC) AS [Saves Ranking],
    a.Assists,
    RANK() OVER (ORDER BY Assists DESC) AS [Assists Ranking],
    a.Shots,
    a.[Shooting Percentage],
    a.Inflicted,
    RANK() OVER (ORDER BY Inflicted DESC) AS [Inflicted Ranking]
    FROM agg_step a
    LEFT JOIN player_index pi
        ON pi.Discord = a.Discord
    WHERE League not like '%Coach%'
    '''
    players = ps.sqldf(query, locals())
    players.to_csv('GCBLeague/LeagueAuto/results.csv')

    return players[['platform_id', 'Player', 'Team', 'Goals', 'Saves', 'Assists', 'Shots',
       'Shooting Percentage', 'Inflicted', 'League',
       'Goals Ranking', 'Assists Ranking', 'Saves Ranking','Inflicted Ranking']]

if __name__ == "__main__":
    inpath = 'GCBLeague/GrindhouseProjects/seasonData.csv'
    indexPath = 'GCBLeague/GrindhouseProjects/S1PlayerInde.csv'

    merged_data = merge_data(inpath, indexPath)
    # merged_data.to_csv('merged_data.csv')

    if debug == 1:
        player_index = pd.read_csv(indexPath)
        get_merged_names = """
        select s1.platform_id,
            s1.distinct_names,
            coalesce(pi1.Discord,pi2.Discord) AS Discord
        FROM (
            SELECT platform_id, 
            GROUP_CONCAT(DISTINCT name) AS distinct_names
            FROM merged_data
            GROUP BY platform_id
        ) s1
        LEFT JOIN player_index pi1 
            ON s1.platform_id = pi1.platform_id
        LEFT JOIN player_index pi2
            ON s1.platform_id = pi2.alt_platform_id
        ORDER BY 3
        """

        all_players_merged = ps.sqldf(get_merged_names,locals())
        all_players_merged.to_csv('GCBLeague/GrindhouseProjects/all_merged_names.csv')
        all_players = pd.read_csv('GCBLeague/GrindhouseProjects/all_names.csv')
        all_players_merged = pd.read_csv('GCBLeague/GrindhouseProjects/all_merged_names.csv')
        differences = all_players.compare(all_players_merged)
        differences = differences.dropna(how='all')
        if (len(differences) > 0):
            print("Players not accounted for: " + str(len(differences)))
            print(differences)
            print()
        else:
            print()
            print(str(len(all_players)-1) + "/"+ str(len(all_players_merged)-1)+" Players accounted for") # -1 for file handling
            print()

    # Create Match History
    match_history = create_matchhistory(merged_data)
    match_history = admin_adjustments(match_history) 
    match_history.to_csv('GCBLeague/GrindhouseProjects/match_history.csv')

    # Create Leaderboard
    leaderboard_df = create_leaderboard(match_history)
    leaderboard_df = leaderboard_df.rename(columns={'Series_Record':'Series Record','Team_Points':'Team Points'})
    leaderboard_df = leaderboard_df.sort_values(by='Team Points',ascending=False)
    leaderboard_df.to_csv('GCBLeague/GrindhouseProjects/Leaderboard.csv')

    # Superlatives - add re-ranking to superlatives to reset the rank
    superlatives = player_superlatives(merged_data,indexPath)
    superlatives.to_csv('GCBLeague/GrindhouseProjects/Superlatives.csv')
    
   