import os
import sys
import urllib
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from argparse import ArgumentParser, RawTextHelpFormatter

YEARS = ['2017', '2018', '2019', '2020', '2021']
MONTHS = list(range(1, 13))
INTERVALS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1mo"]
DAILY_INTERVALS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
TRADING_TYPES = ["spot", "um", "cm"]
MKT_DATA_TYPES = ["klines", "trades", "aggTrades"]
BASE_URL = 'https://data.binance.vision/'

SPOT_AGGTRADES_COLUMNS = [
    "Aggregate_tradeId", "Price", "Quantity", "First_tradeId", "Last_tradeId", "Timestamp",
    "Was_the_buyer_the_maker", "Was_the_trade_the_best_price_match"
]
SPOT_KLINES_COLUMNS = [
    "Open_time", "Open", "High", "Low", "Close", "Volume",
    "Close_time", "Quote_asset_volume", "Number_of_trades",
    "Taker_buy_base_asset_volume", "Taker_buy_quote_asset_volume", "Ignore"
]
SPOT_TRADES_COLUMNS = [
    "trade_Id", "price", "qty", "quoteQty", "time", "isBuyerMaker", "isBestMatch"
]
UM_AGGTRADES_COLUMNS = [
    "Aggregate_tradeId", "Price", "Quantity", "First_tradeId", "Last_tradeId", "Timestamp", "Was_the_buyer_the_maker"
]
CM_AGGTRADES_COLUMNS = UM_AGGTRADES_COLUMNS
UM_KLINES_COLUMNS = SPOT_KLINES_COLUMNS
CM_KLINES_COLUMNS = [
    "Open_time", "Open", "High", "Low", "Close", "Volume",
    "Close_time", "Base_asset_volume", "Number_of_trades",
    "Taker_buy_base_asset_volume", "Taker_buy_quote_asset_volume", "Ignore"
]
UM_TRADES_COLUMNS = SPOT_TRADES_COLUMNS
CM_TRADES_COLUMNS = [
    "trade_Id", "price", "qty", "baseQty", "time", "isBuyerMaker", "isBestMatch"
]
COLUMNS = {
    "spot": {
        "aggTrades": SPOT_AGGTRADES_COLUMNS,
        "klines": SPOT_KLINES_COLUMNS,
        "trades": SPOT_TRADES_COLUMNS
    },
    "um": {
        "aggTrades": UM_AGGTRADES_COLUMNS,
        "klines": UM_KLINES_COLUMNS,
        "trades": UM_TRADES_COLUMNS
    },
    "cm": {
        "aggTrades": CM_AGGTRADES_COLUMNS,
        "klines": CM_KLINES_COLUMNS,
        "trades": CM_TRADES_COLUMNS
    }
}


class BinanceArchive:

    def __init__(self, trading_type, mkt_data_type, symbol, interval, start_date, end_date, destination_dir):
        self.trading_type: str = trading_type
        self.mkt_data_type: str = mkt_data_type
        self.symbol: str = symbol.upper()
        self.interval: str = interval
        self.start_date: datetime = start_date if start_date else datetime.today() - timedelta(days=1)
        self.end_date: datetime = end_date if end_date else datetime.today() - timedelta(days=1)
        self.destination_dir: str = destination_dir if destination_dir else os.getcwd()
        self.dates = []
        self.files = []
        self.path = ""
        self._populate()

    @classmethod
    def from_params(cls, params: dict):
        trading_type: str = params["trading_type"]
        mkt_data_type: str = params["mkt_data_type"]
        symbol: str = params["symbol"]
        interval: str = params.get("interval", None)
        start_date: datetime = params.get("start_date", None)
        end_date: datetime = params.get("end_date", None)
        destination_dir: str = params.get("dir", None)
        result = cls(trading_type, mkt_data_type, symbol, interval, start_date, end_date, destination_dir)
        return result

    def download(self) -> None:
        save_dir = os.path.join(self.destination_dir, self.path)
        if not os.path.exists(save_dir):
            Path(save_dir).mkdir(parents=True, exist_ok=True)
        for f in self.files:
            self._download_file(f)

    def load(self) -> pd.DataFrame:
        df_list = [self._load_dataframe(f) for f in self.files]
        result = pd.concat(df_list)
        return result

    def _populate(self) -> None:
        self.path = self._get_path()
        t = self.start_date
        while t <= self.end_date:
            file = self._get_filename(t)
            self.dates.append(t)
            self.files.append(file)
            t += timedelta(days=1)

    def _get_filename(self, date: datetime) -> str:
        if self.mkt_data_type == "klines":
            filename = "{}-{}-{}.zip".format(self.symbol, self.interval, date.strftime("%Y-%m-%d"))
        else:
            filename = "{}-{}-{}.zip".format(self.symbol, self.mkt_data_type, date.strftime("%Y-%m-%d"))
        return filename

    def _get_path(self) -> str:
        if self.trading_type == 'spot':
            trading_type_path = 'data/spot/'
        else:
            trading_type_path = "data/futures/{}/".format(self.trading_type)
        if self.mkt_data_type == "klines":
            mkt_data_type_path = "daily/klines/{}/{}/".format(self.symbol, self.interval)
        else:
            mkt_data_type_path = "daily/klines/{}/".format(self.symbol)
        path = trading_type_path + mkt_data_type_path
        return path

    def _get_file_path(self, filename):
        return os.path.join(self.destination_dir, self.path, filename)

    def _download_file(self, filename: str) -> None:
        remote_url = BASE_URL + self.path + filename
        save_path = self._get_file_path(filename)
        if os.path.exists(save_path):
            print(f"{save_path} already exists. skip....")
            return
        download_with_buffer(remote_url, save_path)

    def _load_dataframe(self, filename: str) -> pd.DataFrame:
        save_path = self._get_file_path(filename)
        if not os.path.exists(save_path):
            self._download_file(filename)
        column_names = COLUMNS[self.trading_type][self.mkt_data_type]
        df = pd.read_csv(save_path, compression='zip', header=None, names=column_names)
        return df


def download_with_buffer(remote_url: str, save_path: str) -> None:
    try:
        dl_file = urllib.request.urlopen(remote_url)
        length = int(dl_file.getheader('content-length'))
        blocksize = 4096
        if length:
            blocksize = max(blocksize, length // 100)
        dl_progress = 0
        print("\nFile Download: {}".format(save_path))
        with open(save_path, 'wb') as out_file:
            while True:
                buf = dl_file.read(blocksize)
                if not buf:
                    break
                dl_progress += len(buf)
                out_file.write(buf)
                done = int(50 * dl_progress / length)
                sys.stdout.write("\r[%s%s]" % ('#' * done, '.' * (50 - done)))
                sys.stdout.flush()
    except urllib.error.HTTPError:
        print("\nFile not found: {}".format(remote_url))


def get_trading_pairs() -> list:
    r = requests.get("https://api.binance.com/api/v3/exchangeInfo")
    x = r.json()
    symbols = [e["symbol"] for e in x["symbols"]]
    return symbols


def get_parser():
    parser = ArgumentParser(description=f"This is a script to download binance historical data",
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        '-s', dest='symbols', nargs='+', required=True,
        help='Single symbol or multiple symbols separated by space')
    # TODO: handle monthly data and checksum
    # parser.add_argument(
    #     '-y', dest='years', nargs='+', choices=YEARS,
    #     help=('Single year or multiple years separated by space\n'
    #           f"-y 2019 2021 means to download {parser_type} from 2019 and 2021"))
    # parser.add_argument(
    #     '-m', dest='months', nargs='+', type=int, choices=MONTHS,
    #     help=('Single month or multiple months separated by space\n'
    #           f"-m 2 12 means to download {parser_type} from feb and dec"))
    # parser.add_argument(
    #     '-d', dest='dates', nargs='+', type=match_date_regex,
    #     help=('Date to download in [YYYY-MM-DD] format\n'
    #           'single date or multiple dates separated by space\n'
    #           'download past 35 days if no argument is parsed'))
    # parser.add_argument(
    #     '-c', dest='checksum', default=0, type=int, choices=[0, 1],
    #     help='1 to download checksum file, default 0')
    parser.add_argument(
        '--startDate', dest='start_date', type=datetime.fromisoformat, default=datetime.today() - timedelta(days=1),
        help='Starting date to download in [YYYY-MM-DD] format')
    parser.add_argument(
        '--endDate', dest='end_date', type=datetime.fromisoformat, default=datetime.today() - timedelta(days=1),
        help='Ending date to download in [YYYY-MM-DD] format')
    parser.add_argument(
        '--folder', dest='destination_dir', default=os.getcwd(),
        help='Directory to store the downloaded data')
    parser.add_argument(
        '-t', '--tradingType', dest='trading_type', default='spot', choices=TRADING_TYPES,
        help=f"Valid trading types: {TRADING_TYPES}")
    parser.add_argument(
        '--mktDataType', dest='mkt_data_type', default='klines', choices=MKT_DATA_TYPES,
        help=f"Valid market data types: {MKT_DATA_TYPES}"
    )
    # for klines
    parser.add_argument(
        '-i', dest='interval', type=str, choices=DAILY_INTERVALS,
        help=('single kline interval or multiple intervals separated by space\n'
              '-i 1m 1w means to download klines interval of 1minute and 1week'))
    return parser
