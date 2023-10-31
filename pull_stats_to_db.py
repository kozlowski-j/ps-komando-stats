import asyncio
import os
from sqlite3 import OperationalError

from cod_api import API, platforms
import pandas as pd
import sqlite3
import click

from utils import get_date_from_epoch, save_df_to_db

grajki = [
    "ESTERAD#8174023",
    "NOTEVIDIUS#5532788",
    "BORUCHOMIR#7172961",
    "DAWNY-KECZUP#7192166",
    "GITAN#2046256",
    "WORRIED-BLADE115#1720700"
]


async def get_players_combat_history(player_id: str):
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
    # stats = data["playerStats"].apply(pd.Series)
    stats = pd.DataFrame(data["playerStats"].tolist())
    metadata["date"] = metadata["utcStartSeconds"].apply(lambda x: get_date_from_epoch(x))
    stats["ekiadRatio"] = stats["ekiadRatio"].round(2)
    stats["accuracy"] = stats["accuracy"].round(3)
    stats["scorePerMinute"] = stats["scorePerMinute"].round()
    stats["kdRatio"] = stats["kdRatio"].round(3)
    df = pd.concat([metadata, stats], axis=1)
    df = df.sort_values(by="utcStartSeconds", ascending=False)
    return df


def find_last_match(player_name: str, db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT max(utcStartSeconds) FROM stats WHERE player = '{player_name}'")
    result = cursor.fetchone()
    conn.close()
    return int(result[0])


def player_exist_in_db(player_name: str, db_path: str):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT player FROM stats WHERE player = '{player_name}'")
        # Fetch the result
        result = cursor.fetchone()
        conn.close()
        # If result is not None, the table exists
        return result is not None
    except OperationalError as e:
        return False


def create_stats_backup(data_path="data", db_name="cod_stats.db"):
    db_path = os.path.join(data_path, db_name)
    if os.path.exists(db_path):
        import datetime
        now = datetime.datetime.now()
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(sql="SELECT * FROM stats", con=conn)
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        df.to_csv(os.path.join(data_path, f"stats_backup_{now: %Y%m%d%H%M%S}.csv"))
        conn.close()


@click.command()
@click.option("--data_path", default="data", help="Data directory.")
def run_grajki(data_path):
    loop = asyncio.get_event_loop()
    create_stats_backup()
    for player_id in grajki:
        loop.run_until_complete(main(player_id, data_path))


# Create an event loop
async def main(player_id: str, data_path):
    db_path = os.path.join(data_path, "cod_stats.db")
    player_name = player_id.split("#")[0].lower().replace('-', '_')
    print(f"\n{player_name:-^{30}}")
    combat_history = await get_players_combat_history(player_id)
    data = pd.DataFrame(combat_history["data"]["matches"])
    data = data[data["isPresentAtEnd"] == True]
    data['player'] = player_name
    stats_flat = transform_data(data)
    if player_exist_in_db(player_name, db_path):
        last_match_dt = find_last_match(player_name, db_path)
        print("Pulling newest matches...")
        stats_flat = stats_flat[stats_flat['utcStartSeconds'] > last_match_dt].copy()
        if stats_flat.shape[0] == 0:
            print("No new matches since last pull.")
        else:
            save_df_to_db(stats_flat, "stats", "append", db_path=db_path)
    else:
        # Call the function and await its result
        print("Pulling data for the first time...")
        save_df_to_db(stats_flat, "stats", "append", db_path=db_path)


# Create an asyncio event loop and run the main function
if __name__ == "__main__":
    api = API()
    run_grajki()
