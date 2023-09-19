import datetime
import sqlite3


def get_datetime_from_epoch(epoch):
    return datetime.datetime.utcfromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S UTC')


def get_date_from_epoch(epoch):
    return datetime.datetime.utcfromtimestamp(epoch).strftime('%Y-%m-%d')


def today_date_as_epoch():
    """Returns today date (midnight) as epoch."""
    now = datetime.datetime.now()
    midnight = datetime.datetime.combine(now.date(), datetime.time.min)
    epoch_start = datetime.datetime(1970, 1, 1)
    time_since_epoch = midnight - epoch_start
    return int(time_since_epoch.total_seconds())


def save_df_to_db(df, table_name, if_exists, save_index=False):
    # Connect to the SQLite database
    conn = sqlite3.connect("data/cod_stats.db")

    # Append the DataFrame to the SQLite table
    df.to_sql(table_name, conn, if_exists=if_exists, index=save_index)
    conn.commit()
    conn.close()
    print(f"Saved in {table_name}.")


def round_integers(df):
    df.fillna(0, inplace=True)
    for col in df.columns:
        if col not in ['kdRatio', 'ekiadRatio', 'accuracy'] and df[col].dtype != 'O':
            df[col] = df[col].astype(int)
    return df
