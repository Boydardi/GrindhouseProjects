import pandas as pd
from os.path import exists
import pandasql as ps
from datetime import datetime
import uuid
import random
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
        'Traded',
        'TradeDate',
        'TradeTeam',
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
    # df_games.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/results.csv')

    query = """
        WITH TeamCounts AS (
            SELECT id, Team, COUNT(DISTINCT platform_id) AS platform_count
            FROM df_games
            GROUP BY id, Team
        ),
        RankedGames AS (
            SELECT DISTINCT
                df1.*,
                case 
                    when df1.[id] = '711762ab-f3d5-4382-a1b0-28519da1d873' then '2024-12-08T17:49:08-08:00' --BC messed this start time up
                    else df1.[date]
                end as [date_fixed],
                CASE 
                    when df2.Traded = 'Y' and df2.date > df2.TradeDate and df2.Team != df1.Team THEN df2.TradeTeam
                    ELSE df2.Team
                END AS Team_opponent,
                CASE 
                    -- WHEN df1.Traded = 'Y' and df1.date > df1.TradeDate THEN df1.TradeTeam
                    -- WHEN df1.[group_id] = 'c-tier-2yytq7nin4' AND df1.platform_id = '76561198412635657' then 'Tai Lung Leopards' -- Edge case where a team with a traded player and a dedicated alternate play at the same time
                    WHEN df1.Team = 'Alternate' THEN (
                        SELECT Team
                        FROM TeamCounts tc
                        WHERE tc.id = df1.id AND tc.platform_count < 3
                        ORDER BY platform_count DESC
                        LIMIT 1
                    )
                    ELSE df1.Team
                END AS Team_replaced,
                '[' 
                    || CASE
                        WHEN df1.group_id = 'b-tier-i05jpp2ouv' THEN 'Week 3'
                        WHEN df1.group_id = 's-tier-krhmn4sd1o' THEN 'Week 4'
                        WHEN df1.date < '2025-04-29' THEN 'Week 1'
                        WHEN df1.date < '2025-05-06' AND df1.date > '2025-04-29' THEN 'Week 2'
                        WHEN df1.date < '2025-05-13' AND df1.date > '2025-05-06' THEN 'Week 3'
                        WHEN df1.date < '2025-05-20' AND df1.date > '2025-05-13' THEN 'Week 4'
                        WHEN df1.date < '2025-05-27' AND df1.date > '2025-05-20' THEN 'Week 5'
                        WHEN df1.date < '2025-06-03' AND df1.date > '2025-05-27' THEN 'Week 6'
                        WHEN df1.date < '2025-06-10' AND df1.date > '2025-06-03' THEN 'Playoffs R1'
                        WHEN df1.date < '2025-06-17' AND df1.date > '2025-06-10' THEN 'Playoffs R2'
                        ELSE '' END 
                    || ']' 
                    || Case
                        when df1.Traded = 'Y' and df1.date > df1.TradeDate THEN df1.TradeTeam
                        ELSE df1.Team END
                    || ' vs ' 
                    || df2.Team 
                    || '(' 
                    || df1.League 
                    || ')' 
                AS match_name
            FROM df_games df1
            JOIN df_games df2
                ON df2.id = df1.id
                AND df2.created = df1.created
            WHERE df1.Team <> df2.Team
            AND df2.Team <> 'Alternate'
            AND df2.Team <> df1.Team
            AND df1.Score > 2
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
            [date_fixed]as [date],
            [start_time],
            [end_time],
            [Discord],
            [platform],
            [platform_id],
            [name],
            [car_id],
            [car_name],
            Team_replaced AS Team,
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
            Case 
                when [group_id] = 's-tier-kg2memz8lb' then LAST_VALUE(match_name) OVER (PARTITION BY group_id)
                when [group_id] = 'b-tier-urdbj4gqci' then LAST_VALUE(match_name) OVER (PARTITION BY group_id)
                when [group_id] = 'a-tier-eonpxtxguf' then LAST_VALUE(match_name) OVER (PARTITION BY group_id)
                when [group_id] = 's-tier-z2477qtxdg' then LAST_VALUE(match_name) OVER (PARTITION BY group_id)
                when [group_id] = 'a-tier-83e7pv66vb' then LAST_VALUE(match_name) OVER (PARTITION BY group_id)
                when [group_id] = 's-tier-oi0nj2hsg1' then LAST_VALUE(match_name) OVER (PARTITION BY group_id)
                when [group_id] = 'c-tier-2yytq7nin4' then LAST_VALUE(match_name) OVER (PARTITION BY group_id) --A traded with a designated alternate
                ELSE FIRST_VALUE(match_name) OVER (PARTITION BY group_id) 
            END AS match_name,
            DENSE_RANK() OVER (PARTITION BY group_id ORDER BY date_fixed) AS game_number,
            CASE 
                WHEN date < Date('2025-06-03') THEN '5'
                ELSE '7' 
            END AS 'Series_Length'
        FROM RankedGames
        where Team_replaced <> Team_opponent
        ORDER BY date ASC;
    """

    # Execute the SQL query
    df_with_opponent = ps.sqldf(query, locals())
    # print(df_with_opponent.head(12))
    
    df_with_opponent.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/AllMatches.csv')

        # Set game_mode based on match_name
    df_with_opponent['game_mode'] = '3v3'  # Default value
    
    # df_with_opponent.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/AllMatches.csv')

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

    # Get the overall goals scored
    match_history['Goals_Scored_Team_team'] = match_history.groupby('match_name')['team_goals_team'].transform('sum')
    match_history['Goals_Scored_Team_opponent'] = match_history.groupby('match_name')['team_goals_opponent'].transform('sum')
    match_history.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/full_match_history.csv')
    match_history['Points'] = None
    # match_history.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/results.csv')
    rSweepFlag = False
    for index,row in match_history.iterrows():
        game_mode = row['game_mode']
        Team1_Series = row['Series_Record_Team_team']
        Team2_Series = row['Series_Record_Team_opponent']
        Team1_goals = row['Goals_Scored_Team_team']
        Team2_goals = row['Goals_Scored_Team_opponent']

        # print(match_history.at[index, 'match_name'][1:7])
        if match_history.at[index, 'match_name'][1:9] == 'Playoffs': # Playoffs 
            # print(match_history.at[index, 'match_name'][1:9])
            if (Team1_Series < 4 & Team2_Series < 4):
                continue
            elif(((Team1_Series == 4) & (Team2_Series == 0) & (Team2_goals == 0))|((Team2_Series == 4) & (Team1_Series == 0) & (Team1_goals == 0))):
                match_history.at[index, 'Points']= 10
            elif(((Team1_Series == 4) & (Team2_Series == 0))|((Team2_Series == 4) & (Team1_Series == 0))):
                match_history.at[index, 'Points'] = 8
            elif ((Team1_Series == 4)|(Team2_Series == 4)):
                match_history.at[index, 'Points'] = 6

        elif match_history.at[index, 'match_name'][1:7] != 'Week 1': # Regular season
                # print(match_history.at[index, 'match_name'][1:7])
            if (Team1_Series < 3 & Team2_Series < 3):
                continue
            elif(((Team1_Series == 3) & (Team2_Series == 0) & (Team2_goals == 0))|((Team2_Series == 3) & (Team1_Series == 0) & (Team1_goals == 0))):
                match_history.at[index, 'Points']= 10
            elif(((Team1_Series == 3) & (Team2_Series == 0))|((Team2_Series == 3) & (Team1_Series == 0))):
                match_history.at[index, 'Points'] = 8
            elif ((Team1_Series == 3)|(Team2_Series == 3)):
                match_history.at[index, 'Points'] = 6
        elif match_history.at[index, 'match_name'][1:7] == 'Week 1': # Preseason 
                # print(match_history.at[index, 'match_name'][1:7])
            if (Team1_Series < 3 & Team2_Series < 3):
                continue
            elif(((Team1_Series == 3) & (Team2_Series == 0) & (Team2_goals == 0))|((Team2_Series == 3) & (Team1_Series == 0) & (Team1_goals == 0))):
                match_history.at[index, 'Points']= 0.01
            elif(((Team1_Series == 3) & (Team2_Series == 0))|((Team2_Series == 3) & (Team1_Series == 0))):
                match_history.at[index, 'Points'] = 0.01
            elif ((Team1_Series == 3)|(Team2_Series == 3)):
                match_history.at[index, 'Points'] = 0.01
        # match_history.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/results.csv')
    
    match_history = match_history[match_history['Points']>0]
    match_history['Points'] = match_history['Points'].astype(int)
    # match_history.to_csv('GCBLeague/GrindhouseProjects/results.csv')

    final_match_history = match_history.drop(columns=['game_number','team_goals_team','team_goals_opponent'])
    final_match_history.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/full_match_history.csv')
    return final_match_history

def create_leaderboard(df):
    all_teams_performance = pd.concat([df['Team_team'], df['Team_opponent']]).unique()
    all_teams_performance = pd.DataFrame({'Team': all_teams_performance, 'Team_Points': 0, 'Wins': 0, 'Losses': 0})

    #df.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/results.csv')

    all_teams_performance['Wins'] = 0
    all_teams_performance['Losses'] = 0
    all_teams_performance['Series Length'] = 5

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
    all_players.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/all_names.csv')

    # Define the SQL query for the merge
    query = """
    SELECT season_live.*, 
    coalesce(pi1.Team, pi2.Team) AS Team,
    coalesce(pi1.League, pi2.League) AS League,
    coalesce(pi1.Discord,pi2.Discord) AS Discord,
    coalesce(pi1.Traded,pi2.Traded) AS [Traded],
    coalesce(pi1.TradeDate,pi2.TradeDate) AS [TradeDate],
    coalesce(pi1.TradedTeam,pi2.TradedTeam) AS [TradeTeam]
    FROM season_live
    LEFT JOIN player_index pi1 
        ON season_live.platform_id = pi1.platform_id
    LEFT JOIN player_index pi2
        ON season_live.platform_id = pi2.alt_platform_id
    """
    merged_data = ps.sqldf(query, locals())
    merged_data.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/merged_data.csv', index=False)

    return merged_data

def insert_row(df,match_name,Team_team,Team_opponent,Series_Record_Team_team,Series_Record_Team_opponent,Series_Record,game_mode,Goals_Scored_Team_team,Goals_Scored_Team_opponent,Points):
    df.loc[-1] = [match_name,Team_team,Team_opponent,Series_Record_Team_team,Series_Record_Team_opponent,Series_Record,game_mode,Goals_Scored_Team_team,Goals_Scored_Team_opponent,Points]
    df.index = df.index + 1  # shifting index
    df = df.sort_index(ascending=True)
    return df

def create_blank_matches(merged_data, match_name, Created, Team, Gameoutcome, Opponents, games, League):
    # Load the player index
    player_index = pd.read_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/S3PlayerIndex.csv')

    # Get list of unique players from the given League and Team
    players_in_league = player_index[(player_index['League'] == League) & (player_index['Team'].notna())]

    # Separate players by team
    team_players = players_in_league[players_in_league['Team'] == Team]['Discord'].dropna().unique().tolist()
    opp_players = players_in_league[players_in_league['Team'] == Opponents]['Discord'].dropna().unique().tolist()

    # Sample 3 from each side, or fewer if not enough
    selected_team_players = random.sample(team_players, min(3, len(team_players)))
    selected_opp_players = random.sample(opp_players, min(3, len(opp_players)))

    # Time formatting
    created_dt = pd.to_datetime(Created)
    date_str = created_dt.date().isoformat()
    start_time = created_dt.isoformat()
    end_time = created_dt.isoformat()

    # Generate the group_id once per match
    match_group_id = str(uuid.uuid4())

    # Define a template row with all columns from the merged_data
    blank_template = {col: 0 for col in merged_data.columns}
    blank_template.update({
        'match_type': 'Private',
        'team_size': 3,
        'created': Created,
        'date': date_str,
        'start_time': start_time,
        'end_time': end_time,
        'season': 18,
        'season_type': 'free2play',
        'duration': 0,
        'overtime': 0,
        'title': f"{date_str} {Team} Private {Gameoutcome}",
        'match_name': match_name,
        'ETL': 0,
        'League': League,
        'group_id': match_group_id,
        'Series Length':5
    })

    records = []

    # Helper to build one row correctly based on PlayerIndex actual team
    def build_row(player_name, game_id, game_number):
        player_data = player_index[player_index['Discord'] == player_name].iloc[0]
        player_team_actual = player_data['Team']

        if player_team_actual == Team:
            player_team = Team
            player_opp = Opponents
            outcome = Gameoutcome
        elif player_team_actual == Opponents:
            player_team = Opponents
            player_opp = Team
            outcome = 'L' if Gameoutcome == 'W' else 'W' if Gameoutcome == 'L' else Gameoutcome
        else:
            player_team = player_team_actual
            player_opp = Team if player_team_actual != Team else Opponents
            outcome = Gameoutcome

        row = blank_template.copy()
        row.update({
            'uid': str(uuid.uuid4()),
            'id': game_id,  # Shared per game for all players
            'name': player_name,
            'Team': player_team,
            'Team_opponent': player_opp,
            'game_outcome': outcome,
            'platform': str(player_data.get('Platform', '')),
            'platform_id': str(player_data.get('platform_id', '')),
            'Discord': player_name,
            'Traded': player_data.get('Traded', 0),
            'TradeDate': player_data.get('TradeDate', 0),
            'TradeTeam': player_data.get('TradedTeam', 0),
            'game_number': game_number,
            'Series Length':5
        })
        return row

    # Generate records for each game (generate one game_id per game)
    for game_number in range(1, games + 1):
        game_id = str(uuid.uuid4())  # Shared id for this game
        for player in selected_team_players:
            records.append(build_row(player, game_id, game_number))
        for player in selected_opp_players:
            records.append(build_row(player, game_id, game_number))

    generateddf = pd.DataFrame(records)
    print(generateddf.head(6))

    generateddf.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/gen_results.csv', index=False)
    return generateddf

def admin_adjustments(match_history):
    all_matches = pd.read_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/AllMatches.csv')
    match_history = insert_row(match_history,'[Week 2]Nessies vs MobyDicks(C)- MobyDicks FF', 'Nessies','MobyDicks',3,0,3-0,'3v3',0,0,0)
    combined = pd.concat([all_matches, create_blank_matches(merged_data,'[Week 2]Nessies vs MobyDicks(C)','2025-05-05 07:00:00','Nessies','W','MobyDicks',3,'C')], ignore_index=True)
    combined = combined.drop(columns=['Unnamed: 0'])
    combined.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/AllMatches.csv')
    
    all_matches = pd.read_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/AllMatches.csv')
    match_history = insert_row(match_history,'[Week 3]MobyDicks vs Jormungandr(S)- Jormungandr FF', 'MobyDicks','Jormungandr',3,0,3-0,'3v3',0,0,0)
    combined = pd.concat([all_matches, create_blank_matches(merged_data,'[Week 3]MobyDicks vs Jormungandr(S)','2025-05-12 07:00:00','MobyDicks','W','Jormungandr',3,'S')], ignore_index=True)
    combined = combined.drop(columns=['Unnamed: 0'])
    combined.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/AllMatches.csv')

    all_matches = pd.read_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/AllMatches.csv')
    match_history = insert_row(match_history,'[Week 6]Nightstakers vs Jormungandr(C)- Jormungandr FF', 'Nightstalkers','Jormungandr',3,0,3-0,'3v3',0,0,0)
    combined = pd.concat([all_matches, create_blank_matches(merged_data,'[Week 6]Nightstakers vs Jormungandr(C)','2025-06-02 07:00:00','Nightstalkers','W','Jormungandr',3,'C')], ignore_index=True)
    combined = combined.drop(columns=['Unnamed: 0'])
    combined.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/AllMatches.csv')

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
    # players.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/LeagueAuto/results.csv')

    return players[['platform_id', 'Player', 'Team', 'Goals', 'Saves', 'Assists', 'Shots',
       'Shooting Percentage', 'Inflicted', 'League',
       'Goals Ranking', 'Assists Ranking', 'Saves Ranking','Inflicted Ranking']]

if __name__ == "__main__":
    inpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/seasonData.csv'
    indexPath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/S3PlayerIndex.csv'

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
        all_players_merged.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/all_merged_names.csv')
        all_players = pd.read_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/all_names.csv')
        all_players_merged = pd.read_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/all_merged_names.csv')
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
    match_history.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/match_history.csv')

    # Create Leaderboard
    leaderboard_df = create_leaderboard(match_history)
    leaderboard_df = leaderboard_df.rename(columns={'Series_Record':'Series Record','Team_Points':'Team Points'})
    leaderboard_df = leaderboard_df.sort_values(by='Team Points',ascending=False)
    leaderboard_df.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/Leaderboard.csv')

    # Superlatives - add re-ranking to superlatives to reset the rank
    superlatives = player_superlatives(merged_data,indexPath)
    superlatives.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/Superlatives.csv')
    
   