import pandas as pd
from google.cloud import storage
from io import StringIO
from datetime import datetime, timedelta
import streamlit as st
import numpy as np


def read_csv_from_gcs_to_df(bucket_name, file_path):
    """
    Read a CSV file from a Google Cloud Storage bucket and return a DataFrame.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket.
        file_path (str): The path to the CSV file in the Google Cloud Storage bucket.

    Returns:
        pd.DataFrame: The DataFrame containing the data from the CSV file.
    """
    # Initialize a Cloud Storage client
    client = storage.Client.from_service_account_info(st.secrets['SERVICE_ACC'])
    # Get the bucket object
    bucket = client.bucket(bucket_name)

    # Get last update of the file
    blobs_list = list(bucket.list_blobs())

    for bl in blobs_list:
        if bl.name == file_path:
            last_update = bl.updated.strftime('%Y-%m-%d %H:%M:%S')

    # Read the CSV file from Cloud Storage
    blob = bucket.blob(file_path)

    csv_data = blob.download_as_string().decode('utf-8')

    # Parse the CSV data into a DataFrame
    dataframe = pd.read_csv(StringIO(csv_data))

    return dataframe, last_update


def calculate_nearest_fifteen_minute_timestamp(last_update):
    last_update_datetime = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
    if last_update_datetime.minute < 15:
        nearest_fifteen_minute_timestamp = last_update_datetime.replace(minute=45, second=0, microsecond=0) - timedelta(hours=1)
    elif 15 <= last_update_datetime.minute < 30:
        nearest_fifteen_minute_timestamp = last_update_datetime.replace(minute=0, second=0, microsecond=0)
    elif 30 <= last_update_datetime.minute < 45:
        nearest_fifteen_minute_timestamp = last_update_datetime.replace(minute=15, second=0, microsecond=0)
    else:
        nearest_fifteen_minute_timestamp = last_update_datetime.replace(minute=30, second=0, microsecond=0)

    nearest_fifteen_minute_timestamp = nearest_fifteen_minute_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return nearest_fifteen_minute_timestamp

def calculate_nearest_one_hour_timestamp(last_update):
    last_update_datetime = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
    nearest_one_hour_timestamp = (last_update_datetime - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    nearest_one_hour_timestamp = nearest_one_hour_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return nearest_one_hour_timestamp

def calculate_nearest_four_hour_timestamp(last_update):
    last_update_datetime = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
    if last_update_datetime.hour < 4:
        nearest_four_hour_timestamp = last_update_datetime.replace(hour=20, minute=0, second=0, microsecond=0) - timedelta(day=1)
    elif 4 <= last_update_datetime.hour < 8:
        nearest_four_hour_timestamp = last_update_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    elif 8 <= last_update_datetime.hour < 12:
        nearest_four_hour_timestamp = last_update_datetime.replace(hour=4, minute=0, second=0, microsecond=0)
    elif 12 <= last_update_datetime.hour < 16:
        nearest_four_hour_timestamp = last_update_datetime.replace(hour=8, minute=0, second=0, microsecond=0)
    elif 16 <= last_update_datetime.hour < 20:
        nearest_four_hour_timestamp = last_update_datetime.replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        nearest_four_hour_timestamp = last_update_datetime.replace(hour=16, minute=0, second=0, microsecond=0)
    nearest_four_hour_timestamp = nearest_four_hour_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return nearest_four_hour_timestamp

def calculate_nearest_one_day_timestamp(last_update):
    last_update_datetime = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
    nearest_one_day_timestamp = (last_update_datetime - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    nearest_one_day_timestamp = nearest_one_day_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return nearest_one_day_timestamp

def calculate_nearest_one_week_timestamp(last_update):
    last_update_datetime = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
    nearest_one_week_timestamp = (last_update_datetime - timedelta(days=7) - timedelta(days=last_update_datetime.weekday()))
    nearest_one_week_timestamp = nearest_one_week_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    return nearest_one_week_timestamp

def create_timestamp_dataframe(fifteen_minutes,one_hour,four_hour,one_day,one_week):
    timeframes_timestamp = {
        '15m': [fifteen_minutes],
        '1h': [one_hour],
        '4h': [four_hour],
        '1d': [one_day],
        '1w': [one_week]
    }
    # Créer le DataFrame
    timestamp_display_df = pd.DataFrame(timeframes_timestamp)
    timestamp_df = timestamp_display_df.transpose().reset_index().rename(columns={'index': 'timeframe', 0: 'timestamp_datetime'})

    return timestamp_display_df, timestamp_df


def add_calculate_fields(dataframe):
    dataframe['signal'] = np.where(dataframe['buy_signal'] == True, 'BUY', 'SELL')
    dataframe['signal_timeframe'] = dataframe['signal'] + '_' + dataframe['timeframe']
    dataframe['value_exists'] = True

    map_tv_timeframes = {
        '15m':'15',
        '1h':'1H',
        '4h':'4H',
        '1d':'1D',
        '1w':'1W'
    }
    dataframe['link_tv_timeframes'] = "https://www.tradingview.com/chart/?symbol=KUCOIN:"+ dataframe.symbol.str.replace('/','') \
                           +"&interval="+dataframe.timeframe.replace(map_tv_timeframes)

    return dataframe

def create_signals_dataframe(dataframe):
    # Pivot the table
    pivot_table = dataframe.pivot_table(index='symbol',columns='signal_timeframe', values='value_exists', aggfunc='any',
                                 fill_value=False)
    # Filtrer les colonnes pour calcul nombre de signaux
    buy_columns = [col for col in pivot_table.columns if 'BUY' in col]
    sell_columns = [col for col in pivot_table.columns if 'SELL' in col]


    # Appliquer la somme uniquement sur les colonnes filtrées
    pivot_table['TOT'] = pivot_table.sum(axis=1)
    pivot_table['BUY'] = pivot_table[buy_columns].sum(axis=1)
    pivot_table['SELL'] = pivot_table[sell_columns].sum(axis=1)
    pivot_table = pivot_table.reset_index()
    pivot_table['link_tv'] = "https://www.tradingview.com/chart/?symbol=KUCOIN:" + \
                             pivot_table.symbol.str.replace('/','') + "&interval=1D"
    pivot_table = pivot_table.reindex(columns=['link_tv','symbol', 'BUY_15m', 'BUY_1h', 'BUY_4h', 'BUY_1d', 'BUY_1w', 'SELL_15m',
                                               'SELL_1h', 'SELL_4h', 'SELL_1d', 'SELL_1w',
                                               'BUY', 'SELL', 'TOT'])

    pivot_table = pivot_table.dropna(axis=1)

    return pivot_table


