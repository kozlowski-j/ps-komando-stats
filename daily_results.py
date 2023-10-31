import os

import pandas as pd
import sqlite3
from utils import round_integers, today_date_as_epoch
import click

grajki = [
    "ESTERAD#8174023",
    "NOTEVIDIUS#5532788",
    "BORUCHOMIR#7172961",
    "DAWNY-KECZUP#7192166",
    "GITAN#2046256",
    "WORRIED-BLADE115#1720700"
]


def aggregate_stats(data, player_name):
    df = data[(data['utcStartSeconds'] > today_date_as_epoch()) & (data['player'] == player_name)]
    # df = data[(data['player'] == player_name)]

    result_meta = pd.DataFrame({
        "matches": df["matchID"].nunique(),
        "highestMultikill": df["highestMultikill"].max(),
        "scorePerMinuteAvg": df["scorePerMinute"].mean(),
        "highestStreak": df["highestStreak"].max(),
        "mostKillsInMatch": df['kills'].max(),
        "mostDeathsInMatch": df['deaths'].max(),
        "mostObjectivesInMatch": df['objectives'].max()
    }, index=[0])
    sums = pd.DataFrame(df[['kills', 'deaths', 'score', 'damageDealt', 'objectives', 'headshots', 'assists',
               'multikills', 'shotsFired', 'shotsLanded', 'shotsMissed', 'hits', 'timePlayed',
               'suicides', 'shots', 'timePlayedAlive', 'ekia']].sum()).T
    result = pd.concat([result_meta, sums], axis=1)
    result = result.loc[:, (result != 0).any(axis=0)]
    result['kdRatio'] = round(result['kills'] / result['deaths'], 3)
    result['ekiadRatio'] = round(result['ekia'] / result['deaths'], 3)
    result['accuracy'] = round(result['shotsLanded'] / result['shotsFired'], 3)
    result['matchesWon'] = df[df["result"] == "win"].shape[0]
    result['scorePerMatchAvg'] = round(result['score'] / result['matches'], -1)
    result['killsPerMatchAvg'] = round(result['kills'] / result['matches'], 2)
    result = round_integers(result)
    return result[['matches', 'matchesWon', 'kills', 'deaths', 'kdRatio', 'ekiadRatio', 'assists',
                   'scorePerMatchAvg', 'killsPerMatchAvg',
                   'objectives', 'headshots',
                   'highestMultikill', 'multikills', 'highestStreak', 'scorePerMinuteAvg', 'ekia',
                   'score', 'damageDealt',
                   'shotsFired', 'shotsLanded', 'accuracy',
                   'suicides', 'mostKillsInMatch',
                   'mostDeathsInMatch', 'mostObjectivesInMatch']]


@click.command()
@click.option("--data_path")
def main(data_path):
    result = pd.DataFrame()
    conn = sqlite3.connect(os.path.join(data_path, "data/cod_stats.db"))
    df = pd.read_sql(
        sql=f"SELECT * FROM stats",
        con=conn
    )

    for player_id in grajki:
        player_name = player_id.split("#")[0].lower().replace("-", "_")
        df3 = aggregate_stats(df, player_name)
        df3.index = [player_name]
        result = pd.concat([result, df3])

    for col in result.columns:
        result[col] = result[col].astype(str)
    result = result.T
    print(result.to_string())


if __name__ == "__main__":
    main()
