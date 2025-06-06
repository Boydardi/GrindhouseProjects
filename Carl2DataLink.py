import pandas as pd

# Load GameIndex
game_index = pd.read_csv('GCBLeague/GrindhouseProjects/GameIndex.csv')

# Load Player Stats
player_stats = pd.read_csv('GCBLeague/GrindhouseProjects/CARL2_MG_Player_Stats.csv', sep=';', quotechar='"')

# Clean column names
player_stats.columns = player_stats.columns.str.strip().str.lower().str.replace(' ', '_')

# Clean player_id
player_stats['player_id'] = player_stats['player_id'].str.split('|').str[1]

# Optional: convert date fields if needed
player_stats['date'] = pd.to_datetime(player_stats['date'], errors='coerce')
game_index['created'] = pd.to_datetime(game_index['created'], errors='coerce')

# Convert 'game' and 'id' to numeric for join
player_stats['game'] = pd.to_numeric(player_stats['game'], errors='coerce')
game_index['id'] = pd.to_numeric(game_index['id'], errors='coerce')

# Join on game ID (player_stats.game == game_index.id)
merged_df = pd.merge(
    player_stats,
    game_index,
    how='left',
    left_on='game',
    right_on='id'
)

# Preview the result
print(merged_df.head())
merged_df.to_csv('C:/Users/conno/Documents/Coding/GCBLeague/GrindhouseProjects/Carl2Data.csv')