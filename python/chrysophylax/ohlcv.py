import ccxt
import ham.paths as hamp
import ham.time_utils as hamt
import luigi
import os
import pandas as pd
import time


class OHLCV(luigi.Task):
    pair = luigi.Parameter()
    exchange = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()

    def output(self):
        path = hamp.path(hamp.OHLCV, **self.to_str_params())
        path = os.path.join(self.destination_path, path)
        self.target = luigi.LocalTarget(path)
        yield self.target

    def run(self):
        self.target.makedirs()
        exchange = ccxt.__dict__[self.exchange]()
        time.sleep(1)
        exchange.load_markets()
        start_s = "{:%Y-%m-%dT00:00:00.000Z}".format(self.month)
        time.sleep(exchange.rateLimit * 1e-3)
        ohlcv = exchange.fetch_ohlcv(
                    self.pair, self.period,
                    exchange.parse8601(start_s) - 100,
                    limit=32 * hamt.PERIODS[self.period])
        labels = "time,open,high,low,close,volume".split(",")
        ohlcv_df = pd.DataFrame.from_records(ohlcv, columns=labels)
        ohlcv_df = ohlcv_df.drop_duplicates()
        ohlcv_df["time"] = ohlcv_df["time"].apply(exchange.iso8601)
        ohlcv_df["time"] = pd.to_datetime(ohlcv_df["time"])
        ohlcv_df.set_index("time", inplace=True)
        ohlcv_df.sort_index(inplace=True)
        if hamt.ongoing_month(self.month):
            next_m = hamt.latest_full_period(self.period)
        else:
            next_m = hamt.next_month(self.month, False)
        ohlcv_df[self.month:next_m].to_csv(self.target.path,
                                           date_format=hamt.DATE_FORMAT)

