import pandas as pd
from os.path import exists
from datetime import datetime
debug = 0

def addTeammates(df):
    # Group by correct columns: group_id and id
    team_data = df.groupby(['group_id', 'id']).apply(
        lambda group: pd.Series({
            'team_a': ', '.join(group[group['game_outcome'] == 'W']['name'].str.strip().tolist()),
            'team_b': ', '.join(group[group['game_outcome'] == 'L']['name'].str.strip().tolist())
        })
    ).reset_index()

    # Merge the team data back to the original dataframe
    df = df.merge(team_data, on=['group_id', 'id'], how='left')
    return df

def generate_pair_counts(df, outcome=None):
    """Generate teammate pair counts. If outcome is None, count all games."""
    if outcome:
        df = df[df['game_outcome'] == outcome].copy()

    pairs = []

    for _, row in df.iterrows():
        team_col = 'team_a' if row['name'] in row['team_a'] else 'team_b'
        teammates = [name.strip() for name in row[team_col].split(',')]
        player = row['name'].strip()

        for mate in teammates:
            mate = mate.strip()
            if mate and mate != player:
                pairs.append((player, mate))

    pair_df = pd.DataFrame(pairs, columns=['player', 'teammate'])
    pair_df['player'] = pair_df['player'].str.strip()
    pair_df['teammate'] = pair_df['teammate'].str.strip()

    label = 'games_together' if outcome is None else (
        'wins_together' if outcome == 'W' else 'losses_together'
    )

    return pair_df.groupby(['player', 'teammate']).size().reset_index(name=label)

def generate_opponent_counts(df):
    """Generate counts of how often players faced each other and win rate."""
    opponents_data = []

    for _, row in df.iterrows():
        player = row['name'].strip()
        player_team = 'team_a' if player in row['team_a'] else 'team_b'
        opponent_team = 'team_b' if player_team == 'team_a' else 'team_a'
        opponents = [op.strip() for op in row[opponent_team].split(',')]

        win = row['game_outcome'] == 'W'
        for op in opponents:
            if op and op != player:
                opponents_data.append((player, op, 1, int(win)))

    opponents_df = pd.DataFrame(opponents_data, columns=[
        'player', 'opponent', 'games_against', 'wins_against'
    ])

    grouped = opponents_df.groupby(['player', 'opponent']).sum().reset_index()
    grouped['win_rate_against'] = grouped['wins_against'] / grouped['games_against']

    return grouped


if __name__ == "__main__":
    inpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/seasonData.csv'
    outpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/CombineData.csv'
    final_outpath = 'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/PlayerStatsCombined.csv'

    # Load original data
    CombineData = pd.read_csv(inpath)
    CombineData['name'] = CombineData['name'].astype(str).str.strip()

    # Add team columns
    CombineData = addTeammates(CombineData)
    CombineData.to_csv(outpath, index=False)

    # Generate teammate stats
    teammate_wins = generate_pair_counts(CombineData, outcome='W')
    teammate_losses = generate_pair_counts(CombineData, outcome='L')
    teammate_games = generate_pair_counts(CombineData, outcome=None)

    # Merge teammate data
    team_combined = teammate_games.merge(teammate_wins, on=['player', 'teammate'], how='left')
    team_combined = team_combined.merge(teammate_losses, on=['player', 'teammate'], how='left')
    team_combined['wins_together'] = team_combined['wins_together'].fillna(0).astype(int)
    team_combined['losses_together'] = team_combined['losses_together'].fillna(0).astype(int)
    team_combined['win_rate'] = team_combined['wins_together'] / team_combined['games_together']

    # Generate opponent stats
    opponent_combined = generate_opponent_counts(CombineData)

    # Export teammate + opponent stats
    team_combined.to_csv(
        'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/CombineTeammateStats.csv',
        index=False
    )
    opponent_combined.to_csv(
        'C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/CombineOpponentStats.csv',
        index=False
    )

    if debug:
        print("Teammates:")
        print(team_combined.head())
        print("Opponents:")
        print(opponent_combined.head())
