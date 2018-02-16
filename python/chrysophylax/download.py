import ccxt
import copy
import datetime
import indicators
import json
import luigi
import os
import pandas as pd
import utility as ut

from luigi.util import inherits

class DownloadMonthlyOHLCV(luigi.Task):
    pair = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
                os.path.join(self.destination_path,
                    "OHLCV_{}_{}_{date:%Y-%m}.csv".format(
                     self.period.upper(),
                     self.pair.replace("/", "-"),
                     date=self.month)))

    def run(self):
        exchange = ccxt.kraken()
        exchange.load_markets()
        start_s = "{:%Y-%m-%dT00:00:00.000Z}".format(self.month)
        ohlcv = exchange.fetch_ohlcv(
                    self.pair, self.period,
                    exchange.parse8601(start_s) - 100,
                    limit=32 * ut.PERIODS[self.period])
        labels = "time,open,high,low,close,volume".split(",")
        ohlcv_df = pd.DataFrame.from_records(ohlcv, columns=labels)
        ohlcv_df["time"] = ohlcv_df["time"].apply(exchange.iso8601)
        ohlcv_df["time"] = pd.to_datetime(ohlcv_df["time"])
        ohlcv_df.set_index("time", inplace=True)
        ohlcv_df.sort_index(inplace=True)
        next_m = ut.next_month(self.month, False)
        ohlcv_df[self.month:next_m].to_csv(self.output().path)

class Indicator(luigi.Task):
    pair = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()
    COLUMN_NAME = ""
    FILE_NAME = ""
    FN = None

    def column_name(self):
        return self.COLUMN_NAME

    def output(self):
        return luigi.LocalTarget(
                os.path.join(self.destination_path,
                    "{}_{}_{}_{date:%Y-%m}.csv".format(
                    self.FILE_NAME,
                    self.period.upper(),
                    self.pair.replace("/", "-"),
                    date=self.month)))

    def run(self):
        data = ut.input_df(self)
        name = self.column_name()
        data[name] = self.FN(data)
        next_m = ut.next_month(self.month, False)
        data = data[self.month:next_m]
        data[[name]].to_csv(self.output().path)

@inherits(Indicator)
class WindowedIndicator(Indicator):
    window_size = luigi.IntParameter()

    def column_name(self):
        return self.COLUMN_NAME + "_{}".format(self.window_size)

    def requires(self):
        for t in self.requires_ohlcv():
            yield t

    def requires_ohlcv(self):
        for m in ut.required_months(self.month, self.window_size, self.period):
            yield DownloadMonthlyOHLCV(
                    self.pair, m, self.period, self.destination_path)

    def output(self):
        return luigi.LocalTarget(
                os.path.join(self.destination_path,
                    "{}-{}_{}_{}_{date:%Y-%m}.csv".format(
                    self.FILE_NAME,
                    self.window_size,
                    self.period.upper(),
                    self.pair.replace("/", "-"),
                    date=self.month)))


@inherits(WindowedIndicator)
class MaxInWindow(WindowedIndicator):
    COLUMN_NAME = "max"
    FILE_NAME = "MAX-IN-WINDOW"
    FN = indicators.max_in_window

@inherits(WindowedIndicator)
class MinInWindow(WindowedIndicator):
    COLUMN_NAME = "min"
    FILE_NAME = "MIN-IN-WINDOW"
    FN = indicators.min_in_window

@inherits(Indicator)
class TrueRange(Indicator):
    COLUMN_NAME = "true_range"
    FILE_NAME = "TR"
    FN = indicators.true_range

    def requires(self):
        yield DownloadMonthlyOHLCV(
                self.pair, ut.previous_month(self.month), self.period, self.destination_path)
        yield DownloadMonthlyOHLCV(
                self.pair, self.month, self.period, self.destination_path)

@inherits(WindowedIndicator)
class AverageTrueRange(WindowedIndicator):
    COLUMN_NAME = "atr"
    FILE_NAME = "ATR"
    FN = indicators.average_true_range

    def requires(self):
        for m in ut.required_months(self.month, self.window_size, self.period):
            yield TrueRange(
                    self.pair, m, self.period, self.destination_path)


class Strategy(luigi.Task):
    pair = luigi.Parameter()
    period = luigi.Parameter(default="1d")
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()
    balance = luigi.FloatParameter(default=100000.0)
    pyramiding = luigi.IntParameter(default=1)
    max_trade_percentage = luigi.FloatParameter(1.0)
    destination_path = luigi.Parameter()

    FILE_NAME = ""
    FN = None

    def run(self):
        data = ut.input_df(self)
        data = self.FN(data)
        data.to_csv(self.output().path)

    def output(self):
        return luigi.LocalTarget(
                os.path.join(self.destination_path,
                    "{}_{}_{}_{:%Y-%m-%d}_{:%Y-%m-%d}.csv".format(
                    self.FILE_NAME,
                    self.period.upper(),
                    self.pair.replace("/", "-"),
                    self.start_date,
                    self.end_date)))


@inherits(Strategy)
class SimpleTurtle(Strategy):
    FILE_NAME = "SIMPLE_TURTLE"
    FN = indicators.simple_turtle

    def requires(self):
        for m in ut.months(self.start_date, self.end_date):
            yield DownloadMonthlyOHLCV(
                    self.pair, m, self.period, self.destination_path)
            yield MaxInWindow(
                    self.pair, m, self.period, self.destination_path, 20)
            yield MinInWindow(
                    self.pair, m, self.period, self.destination_path, 20)
            yield MaxInWindow(
                    self.pair, m, self.period, self.destination_path, 55)
            yield MinInWindow(
                    self.pair, m, self.period, self.destination_path, 55)
            yield MaxInWindow(
                    self.pair, m, self.period, self.destination_path, 10)
            yield MinInWindow(
                    self.pair, m, self.period, self.destination_path, 10)
