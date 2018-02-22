import matplotlib
matplotlib.use('Agg')

import ccxt
import copy
import datetime
import indicators
import json
import luigi
import matplotlib.dates as mdates
import matplotlib.pylab as plt
import numpy as np
import os
import pandas as pd
import plotting as pt
import time
import utility as ut


from luigi.util import inherits
from matplotlib import style
from matplotlib.ticker import AutoMinorLocator
from matplotlib.finance import candlestick_ohlc


class OHLCV(luigi.Task):
    pair = luigi.Parameter()
    exchange = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.destination_path,
                                              ut.task_filename(self, "csv")))

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
        print(
                    self.pair, self.period,
                    exchange.parse8601(start_s) - 100,
                    32 * ut.PERIODS[self.period])
        labels = "time,open,high,low,close,volume".split(",")
        ohlcv_df = pd.DataFrame.from_records(ohlcv, columns=labels)
        ohlcv_df = ohlcv_df.drop_duplicates()
        ohlcv_df["time"] = ohlcv_df["time"].apply(exchange.iso8601)
        ohlcv_df["time"] = pd.to_datetime(ohlcv_df["time"])
        ohlcv_df.set_index("time", inplace=True)
        ohlcv_df.sort_index(inplace=True)
        next_m = ut.next_month(self.month, False)
        ohlcv_df[self.month:next_m].to_csv(self.output().path)

class Indicator(luigi.Task):
    pair = luigi.Parameter()
    exchange = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()
    COLUMN_NAME = ""
    FILE_NAME = ""
    FN = None

    def column_name(self):
        return self.COLUMN_NAME

    def output(self):
        return luigi.LocalTarget(os.path.join(self.destination_path,
                                              ut.task_filename(self, "csv")))

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
            yield OHLCV(self.pair, self.exchange, m, self.period,
                                       self.destination_path)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.destination_path,
                                              ut.task_filename(self, "csv")))


@inherits(WindowedIndicator)
class MaxInWindow(WindowedIndicator):
    COLUMN_NAME = "max"
    FN = indicators.max_in_window

@inherits(WindowedIndicator)
class MinInWindow(WindowedIndicator):
    COLUMN_NAME = "min"
    FN = indicators.min_in_window

@inherits(Indicator)
class TrueRange(Indicator):
    COLUMN_NAME = "true_range"
    FN = indicators.true_range

    def requires(self):
        yield OHLCV(self.pair, self.exchange,
                                   ut.previous_month(self.month), self.period,
                                   self.destination_path)
        yield OHLCV(self.pair, self.exchange,
                                   self.month, self.period,
                                   self.destination_path)

@inherits(WindowedIndicator)
class AverageTrueRange(WindowedIndicator):
    COLUMN_NAME = "atr"
    FN = indicators.average_true_range

    def requires(self):
        for m in ut.required_months(self.month, self.window_size, self.period):
            yield TrueRange(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)

@inherits(WindowedIndicator)
class VolumeMovingAverage(WindowedIndicator):
    COLUMN_NAME = "volume_ma"
    FN = indicators.volume_ma

class Strategy(luigi.Task):
    pair = luigi.Parameter()
    period = luigi.Parameter(default="1d")
    exchange = luigi.Parameter()
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()
    balance = luigi.FloatParameter(default=100000.0)
    pyramiding = luigi.IntParameter(default=1)
    max_trade_percentage = luigi.FloatParameter(default=1.0)
    allow_shorts = luigi.BoolParameter(default=False)
    destination_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.destination_path,
                                              ut.task_filename(self, "csv")))

@inherits(Strategy)
class StrategyFlags(Strategy):
    FN = None

    def run(self):
        data = ut.input_df(self)
        self.FN(data)
        data.to_csv(self.output().path)


@inherits(Strategy)
class StrategyRun(Strategy):
    FN = None

    def run(self):
        data = pd.read_csv(self.requires().output().path,
                           index_col=0, parse_dates=True)
        trades = indicators.execute_strategy(self, data, self.FN)
        trades.to_csv(self.output().path)

@inherits(StrategyFlags)
class SimpleTurtleFlags(StrategyFlags):
    entry = luigi.IntParameter(default=20)
    exit = luigi.IntParameter(default=10)
    FN = indicators.turtle_prepare_signals

    def requires(self):
        for m in ut.months(self.start_date, self.end_date):
            yield OHLCV(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)
            for days in set([self.entry, self.exit]):
                yield MaxInWindow(self.pair, self.exchange, m, self.period,
                                  self.destination_path, days)
                yield MinInWindow(self.pair, self.exchange, m, self.period,
                                  self.destination_path, days)
            yield AverageTrueRange(self.pair, self.exchange, m, self.period,
                                   self.destination_path, 20)

@inherits(SimpleTurtleFlags)
class SimpleTurtle(StrategyRun):
    stop_loss_multiplier = luigi.FloatParameter(default=0.0)
    trailing_stop_multiplier = luigi.FloatParameter(default=0.0)
    FN = indicators.turtle_collect_signals

    def requires(self):
        return SimpleTurtleFlags.from_str_params(self.to_str_params())


@inherits(StrategyFlags)
class BuyAndHoldFlags(StrategyFlags):
    FN = indicators.buy_and_hold_prepare_signals

    def requires(self):
        for m in ut.months(self.start_date, self.end_date):
            yield OHLCV(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)

@inherits(BuyAndHoldFlags)
class BuyAndHold(StrategyRun):
    FN = indicators.buy_and_hold_collect_signals

    def requires(self):
        return BuyAndHoldFlags.from_str_params(self.to_str_params())

@inherits(SimpleTurtle)
class TurtlePlot(luigi.Task):
    volume_window = luigi.IntParameter(default=30)

    def requires(self):
        self.turtle = SimpleTurtle.from_str_params(self.to_str_params())
        yield self.turtle
        vma_params = self.to_str_params()
        vma_params["window_size"] = self.volume_window
        # self.vma = VolumeMovingAverage.from_str_params(vma_params)
        # yield self.vma

    def run(self):
        style.use("ggplot")
        data = pd.read_csv(self.turtle.requires().output().path,
                           index_col=0, parse_dates=True)
        # volume_ma = pd.read_csv(self.vma.output().path, index_col=0,
                                # parse_dates=True)
        data = data.tail(100)
        ohlcv = data[["open", "high", "low", "close", "volume"]]
        ohlcv["time"] = pd.to_datetime(ohlcv.index)
        ohlcv["time"] = ohlcv["time"].apply(lambda d: mdates.date2num(d.to_pydatetime()))
        ohlc = ohlcv[["time", "open", "high", "low", "close", "volume"]].values
        fig, ax1 = plt.subplots(figsize=(16, 9), dpi=120)
        title = "Turtle {exchange}: {pair} ({period})\n" \
                "(entry={entry}, exit={exit})".format(**self.to_str_params())
        plt.title(title.upper(), loc="left")
        pt.configure_date_axis(ax1, self.period)
        mx = np.max(ohlc[:, -1])
        ax1.set_ylim(mx * -0.1, mx * 8)
        ax1.get_yaxis().set_visible(False)
        up = ohlc[:, 1] <= ohlc[:, 4]
        down = ohlc[:, 1] > ohlc[:, 4]
        ax1.bar(ohlc[up, 0], ohlc[up, -1], 0.6 / ut.PERIODS[self.period], color="g", alpha=0.5)
        ax1.bar(ohlc[down, 0], ohlc[down, -1],0.6 / ut.PERIODS[self.period], color="r", alpha=0.5)
        # volume_ma.tail(100).plot()

        ax2 = ax1.twinx()
        ax2.tick_params(axis='both', which='major', labelsize=8)
        ax2.set_ylim(np.min(ohlc[:, 3]) * 0.8, np.max(ohlc[:, 2] * 1.2))
        candlestick_ohlc(ax2, ohlc, colorup="g", width=0.6 / ut.PERIODS[self.period])
        max_entry = "max_{}".format(self.entry)
        min_entry = "min_{}".format(self.entry)
        max_exit = "max_{}".format(self.exit)
        min_exit = "min_{}".format(self.exit)
        ax2.fill_between(data.index.values, data[min_exit].values,
                         data[max_entry].values, lw=0., alpha=0.2,
                         facecolor="chartreuse")
        ax2.fill_between(data.index.values, data[min_entry].values,
                         data[max_exit].values, lw=0., alpha=0.2,
                         facecolor="indianred")
        plt.savefig(self.output().path, bboxinches="tight")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.destination_path,
                                              ut.task_filename(self, "png",
                                                               ["destination_path", "volume_window"])))

