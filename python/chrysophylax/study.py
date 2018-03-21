import ham.paths as hamp
import ham.time_utils as hamt
import luigi
from ohlcv import OHLCV
import os
import pandas as pd
import pytz
import strategies as ds

from gilles.signal_checker import SignalChecker
from gilles import CSVSource
from gilles import Trader
from luigi.util import inherits


class Study(luigi.Task):
    markets = luigi.Parameter()
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()
    start_balance = luigi.FloatParameter(default=100000.0)
    risk_percentage = luigi.FloatParameter(default=0.01)
    destination_path = luigi.Parameter()

    def requires(self):
        if hasattr(self, "ohlcv") and self.ohlcv:
            for t in self.ohlcv:
                yield t
            for t in self.signal_thresholds:
                yield t
            return
        market_df = pd.read_csv(self.markets, index_col=0, parse_dates=True)
        self.ohlcv = []
        self.signal_thresholds = []
        for m in hamt.months(self.start_date, self.end_date):
            for market_row in market_df.itertuples():
                args = dict(pair=market_row.pair,
                            exchange=market_row.exchange,
                            month=m,
                            period=market_row.period,
                            destination_path=self.destination_path)
                task = OHLCV(**args)
                self.ohlcv.append(task)
                yield task
                args["date"] = "{}".format(m)
                task = hamt.init_class("signal", market_row, **args)
                self.signal_thresholds.append(task)
                yield task


    def output(self):
        parms = self.to_str_params()
        cls = self.__class__.__name__
        parms["class"] = cls
        path = hamp.path(hamp.DEFINITIONS[cls], **parms)
        path = os.path.join(self.destination_path, path)
        self.target = luigi.LocalTarget(path)
        yield self.target

    def merge_ohlcv(self):
        self.target.makedirs()
        tmp = []
        for t in self.ohlcv:
            df = pd.read_csv(t.target.path, parse_dates=True)
            df["exchange"] = t.exchange
            df["pair"] = t.pair
            df["period"] = t.period
            tmp.append(df)
        ohlcv_df = pd.concat(tmp)
        ohlcv_df["time"] = pd.to_datetime(ohlcv_df["time"])
        ohlcv_df.set_index("time", inplace=True)
        ohlcv_df.sort_index(level=0, inplace=True)
        ohlcv_df.reset_index(inplace=True)
        return ohlcv_df

    def run(self):
        ohlcv_df = self.merge_ohlcv()
        price_source = CSVSource(ohlcv_df,
                                 self.start_date, self.end_date,
                                 self.destination_path)
        signal_checker = SignalChecker(self.markets, self.destination_path)
        trader = Trader(self.start_balance, self.risk_percentage, pyramiding=4.0)
        for price_row in price_source:
            for signal_row in signal_checker.check(price_row):
                if signal_row is not None:
                    trader.update(signal_row)
        result = trader.closed[:]
        for k, v in trader.open.items():
            for trade in v:
                result.append(trade)
        result_df = pd.DataFrame(result)
        result_df = result_df[trader.FIELD_NAMES]
        result_df.to_csv(self.target.path)
