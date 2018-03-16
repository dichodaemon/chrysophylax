import ccxt
import ham.time_utils as hamt
# import dives.downloads as downloads
import pandas as pd
import time

from ccxt.base.errors import RequestTimeout
from ccxt.base.errors import ExchangeNotAvailable


class DataSource(object):
    def send(self, value):
        return self.__fetch_data__()

    def throw(self, type=None, value=None, traceback=None):
        raise StopIteration

    def __iter__(self):
        return self

    def next(self):
        return self.send(None)

    def close(self):
        try:
            self.throw(GeneratorExit)
        except (GeneratorExit, StopIteration):
            pass
        else:
            raise RuntimeError("Generator ignored GeneratorExit")


class LiveSource(DataSource):
    def __init__(self, ticker_file, data_dir):
        self.data_dir = data_dir
        self.ticker_df = pd.read_csv(ticker_file, index_col=0, parse_dates=True)
        self.ticker_count = len(self.ticker_df)
        self.current_ticker = 0
        self.exchanges = {}

    def exchange(self, name):
        if name not in self.exchanges:
            exchange = getattr(ccxt, name)()
            exchange.load_markets()
            self.exchanges[name] = exchange
        return self.exchanges[name]

    def __fetch_data__(self):
        result = {}
        done = False
        while not done:
            row = self.ticker_df.iloc[self.current_ticker]
            self.current_ticker += 1
            self.current_ticker %= self.ticker_count
            ex = self.exchange(row.exchange)
            try:
                ticker = ex.fetch_ticker(row.pair)
                ticker_time = pd.to_datetime(ex.iso8601(ticker["timestamp"]))
                result = dict(time=ticker_time, exchange=row.exchange,
                              pair=row.pair,
                              price=ticker["last"])
                seconds = ex.rateLimit * 1e-3
                time.sleep(seconds)
                done = True
            except RequestTimeout, ExchangeNotAvailable:
                pass
        return pd.DataFrame([result]).iloc[0]
