import pandas as pd
from google.cloud import storage
from io import StringIO
import numpy as np
import streamlit as st
from st_aggrid import AgGrid,GridOptionsBuilder

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
    df = pd.read_csv(StringIO(csv_data))

    return df, last_update

df,last_update = read_csv_from_gcs_to_df(st.secrets['BUCKET_NAME'],'signals_temp_df.csv')
df['signal'] = np.where(df['buy_signal'] == True,'BUY','SELL')
mapping_timeframes = {
    '15m':'1 - 15m',
    '1h':'2 - 1h',
    '4h':'3 - 4h',
    '1d':'4 - 1d',
    '1w':'5 - 1w'
}

df['timeframe_ordered'] = df['timeframe'].map(mapping_timeframes)

st.markdown(f'<div style="text-align: center;">Wave trend oscillator signals calculated on Kucoin USDT (top 100 crypto from Coinmarketcap)</div>', unsafe_allow_html=True)
st.markdown(f'<div style="text-align: center;">last update : {last_update}</div>', unsafe_allow_html=True)
st.write('----------------------------------------')

gb = GridOptionsBuilder()

gb.configure_auto_height()

# makes columns resizable, sortable and filterable by default
gb.configure_default_column(
    resizable=True,
    filterable=True,
    sortable=True,
    editable=False,
)

#configures symbols column to have a 100px initial width
gb.configure_column(field="symbol",
                    header_name="Symbols",
                    width=1,
                    rowGroup=True
                    )



gb.configure_column(
    field="signal",
    header_name="signal",
    pivot=True,
)

#configures timeframe column to have a tooltip and adjust to fill the grid container
gb.configure_column(
    field="timeframe_ordered",
    header_name="timeframe",
    pivot=True
)


gb.configure_column(
    field="buy_signal",
    header_name="nb_symbol",
    type=["numericColumn"],
    aggFunc="count",
)
#makes tooltip appear instantly
gb.configure_grid_options(
    tooltipShowDelay=0,
    pivotMode=True,
    suppressExpandablePivotGroups = True
)
go = gb.build()

AgGrid(df, gridOptions=go)

