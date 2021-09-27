import pandas as pd
import numpy as np
from util import BinanceArchive, get_parser, COLUMNS
from datetime import datetime

spot_params = {
    "symbol": "BTCUSDT",
    "trading_type": 'spot',
    "mkt_data_type": 'klines',
    "interval": '1h',
    "start_date": datetime(2021, 6, 26),
    "end_date": datetime(2021, 9, 24)
}
spot_ba = BinanceArchive.from_params(spot_params)
spot_df = spot_ba.load()
spot_df = spot_df[["Open_time", "Open", "High", "Low", "Close", "Volume"]]
spot_df.rename(columns={"Open_time": "timestamp",
                        "Open": "spot_open",
                        "High": "spot_high",
                        "Low": "spot_low",
                        "Close": "spot_close",
                        "Volume": "spot_volume"},
               inplace=True)
print(spot_df.head())

futures_params = {
    "symbol": "BTCUSDT_210924",
    "trading_type": 'um',
    "mkt_data_type": 'klines',
    "interval": '1h',
    "start_date": datetime(2021, 6, 26),
    "end_date": datetime(2021, 9, 24)
}
futures_ba = BinanceArchive.from_params(futures_params)
futures_df = futures_ba.load()
futures_df = futures_df[["Open_time", "Open", "High", "Low", "Close", "Volume"]]
futures_df.rename(columns={"Open_time": "timestamp",
                           "Open": "futures_open",
                           "High": "futures_high",
                           "Low": "futures_low",
                           "Close": "futures_close",
                           "Volume": "futures_volume"},
                  inplace=True)
print(futures_df.head())

df = pd.merge(spot_df, futures_df, on="timestamp")
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
maturity = datetime(2021, 9, 24, 23, 59, 59)
df['days_to_maturity'] = (maturity - df['timestamp']).dt.days
df = df[df['days_to_maturity'] > 0]
# assuming simple compounding and zero interest rates, calculate funding spreads
# df['funding_spread'] = (df['futures_close'] / df['spot_close'] - 1) / df['days_to_maturity'] * 365
# assuming exponential compounding and zero interest rates, calculate funding spreads
df['funding_spread'] = np.log(df['futures_close'] / df['spot_close']) / (df['days_to_maturity'] / 365)

print(df.describe())
