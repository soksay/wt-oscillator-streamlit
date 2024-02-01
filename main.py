
from data_utilis import *

# get the data from gcs
df, last_update = read_csv_from_gcs_to_df(st.secrets['BUCKET_NAME'], 'signals_df.csv')

# calculating nearest timestamps for each timeframe based on bucket last update
fifteen_minute_timestamp = calculate_nearest_fifteen_minute_timestamp(last_update)
one_hour_timestamp = calculate_nearest_one_hour_timestamp(last_update)
four_hour_timestamp = calculate_nearest_four_hour_timestamp(last_update)
one_day_timestamp = calculate_nearest_one_day_timestamp(last_update)
one_week_timestamp = calculate_nearest_one_week_timestamp(last_update)

timestamp_display_df, timestamp_df = create_timestamp_dataframe(fifteen_minute_timestamp, one_hour_timestamp,
                                                                four_hour_timestamp, one_day_timestamp,
                                                                one_week_timestamp)
df = df.merge(timestamp_df,how='inner',on=['timeframe','timestamp_datetime'])
df = add_calculate_fields(df)

# Pivot the table
active_signals_df = create_signals_dataframe(df)

st.markdown(
    f'<div style="text-align: center;">Wave trend oscillator signals calculated on Kucoin USDT pairs (top 100 crypto from Coinmarketcap)</div>',
    unsafe_allow_html=True)
st.markdown(f'<div style="text-align: center;">last update : {last_update}</div>', unsafe_allow_html=True)
st.write('----------------------------------------')

# Adding a title to the DataFrame
st.write('Timestamps to consider for each timeframe (refer to the opening timestamp of the candle)')
st.dataframe(timestamp_display_df, hide_index=True,width=1000 )

on = st.toggle('Display raw data')

if on:
    st.dataframe(
        df[['symbol', 'timeframe', 'timestamp_datetime', 'signal', 'PriceUSD', 'WT1', 'WT2', 'MarketCapPosition']],
        use_container_width=True, hide_index=True,width=1000, height=(len(df) + 1) * 35 + 3)
else:
    st.dataframe(active_signals_df, hide_index=True, width=1000, height=(len(active_signals_df) + 1) * 35 + 3)

