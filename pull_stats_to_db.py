import asyncio
from cod_api import API, platforms
import pandas as pd
import sqlite3
import time

from utils import get_date_from_epoch, save_df_to_db

grajki = [
    "ESTERAD#8174023",
    "NOTEVIDIUS#5532788",
    "BORUCHOMIR#7172961",
    "DAWNY-KECZUP#7192166",
    "GITAN#2046256",
    "WORRIED-BLADE115#1720700"
]


async def get_players_combat_history(player_id):
    # login in with sso token
    await api.loginAsync('NDU4NDg1NTE3MTc2NjQ1ODM1NToxNjk1NDMwNzE1MzgyOmZiOTMzMDEwNmI4ZjNhMDYyNjFmYzA3Mzc2OWZmMTQw')
    # retrieving combat history
    hist = await api.ColdWar.combatHistoryAsync(
        platform=platforms.Activision,
        gamertag=player_id
    )  # returns data of type dict
    return hist


def transform_data(data):
    metadata = data[["player", "matchID", "result", "utcStartSeconds", "utcEndSeconds", "map", "mode"]].copy()
    stats = data["playerStats"].apply(pd.Series)
    metadata["date"] = metadata["utcStartSeconds"].apply(lambda x: get_date_from_epoch(x))
    stats["ekiadRatio"] = stats["ekiadRatio"].round(2)
    stats["accuracy"] = stats["accuracy"].round(3)
    stats["scorePerMinute"] = stats["scorePerMinute"].round()
    stats["kdRatio"] = stats["kdRatio"].round(3)
    df = pd.concat([metadata, stats], axis=1)
    df = df.sort_values(by="utcStartSeconds", ascending=False)
    return df


def find_last_match(player_name):
    conn = sqlite3.connect("data/cod_stats.db")
    cursor = conn.cursor()
    cursor.execute(f"SELECT max(utcStartSeconds) FROM stats WHERE player = '{player_name}'")
    result = cursor.fetchone()
    conn.close()
    return int(result[0])


def player_exist_in_db(player_name):
    conn = sqlite3.connect("data/cod_stats.db")
    cursor = conn.cursor()
    cursor.execute(f"SELECT player FROM stats WHERE player = '{player_name}'")
    # Fetch the result
    result = cursor.fetchone()
    conn.close()
    # If result is not None, the table exists
    return result is not None


def create_stats_backup():
    import datetime
    today = datetime.date.today()
    conn = sqlite3.connect("data/cod_stats.db")
    df = pd.read_sql(sql="SELECT * FROM stats", con=conn)
    df.to_csv(f"data/stats_backup_{today: %Y%m%d%H%M%S}.csv")
    conn.close()


# Create an event loop
async def main(player_id):
    player_name = player_id.split("#")[0].lower().replace('-', '_')
    print("-" * 30, '\n', player_name)
    combat_history = await get_players_combat_history(player_id)
    data = pd.DataFrame(combat_history["data"]["matches"])
    data = data[data["isPresentAtEnd"] == True]
    data['player'] = player_name
    stats_flat = transform_data(data)
    if player_exist_in_db(player_name):
        last_match_dt = find_last_match(player_name)
        print("Pulling newest matches...")
        stats_flat = stats_flat[stats_flat['utcStartSeconds'] > last_match_dt].copy()
        if stats_flat.shape[0] == 0:
            print("No new matches since last pull.")
        else:
            save_df_to_db(stats_flat, "stats", "append")
    else:
        # Call the function and await its result
        print("Pulling data for the first time...")
        save_df_to_db(stats_flat, "stats", "append")


# Create an asyncio event loop and run the main function
if __name__ == "__main__":
    api = API()
    loop = asyncio.get_event_loop()
    create_stats_backup()
    for player_id in grajki:
        loop.run_until_complete(main(player_id))
