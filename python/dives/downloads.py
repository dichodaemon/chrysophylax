import ccxt
import luigi
import os
import pandas as pd
import time
import utility as ut

from luigi.contrib.simulate import RunAnywayTarget


class OHLCV(luigi.Task):
    pair = luigi.Parameter()
    exchange = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()

    def output(self):
        if ut.ongoing_month(self.month):
            path = os.path.join(self.destination_path,
                                ut.task_filename(self, "csv", suffix="PARTIAL"))
            self.target = luigi.LocalTarget(path)
            yield self.target
            self.rerun = RunAnywayTarget(self)
            yield self.rerun
        else:
            path = os.path.join(self.destination_path,
                                ut.task_filename(self, "csv"))
            self.target = luigi.LocalTarget(path)
            yield self.target
            self.rerun = None

    def run(self):
        exchange = ccxt.__dict__[self.exchange]()
        time.sleep(1)
        exchange.load_markets()
        start_s = "{:%Y-%m-%dT00:00:00.000Z}".format(self.month)
        time.sleep(1)
        ohlcv = exchange.fetch_ohlcv(
                    self.pair, self.period,
                    exchange.parse8601(start_s) - 100,
                    limit=32 * ut.PERIODS[self.period])
        labels = "time,open,high,low,close,volume".split(",")
        ohlcv_df = pd.DataFrame.from_records(ohlcv, columns=labels)
        ohlcv_df = ohlcv_df.drop_duplicates()
        ohlcv_df["time"] = ohlcv_df["time"].apply(exchange.iso8601)
        ohlcv_df["time"] = pd.to_datetime(ohlcv_df["time"])
        ohlcv_df.set_index("time", inplace=True)
        ohlcv_df.sort_index(inplace=True)
        next_m = ut.next_month(self.month, False)
        ohlcv_df[self.month:next_m].to_csv(self.target.path)
        if self.rerun is not None:
            self.rerun.done()

