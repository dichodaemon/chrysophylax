import matplotlib
matplotlib.use('Agg')

import garm.plotting as garmp
import datetime
import indicators
import luigi
import matplotlib.dates as mdates
import matplotlib.pylab as plt
import numpy as np
import os
import pandas as pd
import strategies
import time
# import utility as ut


from luigi.util import inherits
from matplotlib import style
from simple_turtle import SimpleTurtleSignalThresholds



@inherits(SimpleTurtleSignalThresholds)
class TurtlePlot(luigi.Task):
    volume_window = luigi.IntParameter(default=30)
    price_fast_window = luigi.IntParameter(default=30)
    price_slow_window = luigi.IntParameter(default=50)

    def requires(self):
        params = self.to_str_params()
        self.turtle = SimpleTurtleSignalThresholds.from_str_params(params)
        yield self.turtle
        vma_params = self.to_str_params()
        vma_params["window_size"] = self.volume_window
        self.vma = []
        for t in ut.monthly_task(self.to_str_params(),
                                 indicators.MovingAverage,
                                 self.start_date, self.end_date,
                                 input_column="volume",
                                 window_size=self.volume_window):
            self.vma.append(t)
            yield t
        self.fast_ma = []
        for t in ut.monthly_task(self.to_str_params(),
                                 indicators.MovingAverage,
                                 self.start_date, self.end_date,
                                 input_column="close",
                                 window_size=self.price_fast_window):
            self.fast_ma.append(t)
            yield t
        self.slow_ma = []
        for t in ut.monthly_task(self.to_str_params(),
                                 indicators.MovingAverage,
                                 self.start_date, self.end_date,
                                 input_column="close",
                                 window_size=self.price_slow_window):
            self.slow_ma.append(t)
            yield t
        self.eff_ratio = []
        for t in ut.monthly_task(self.to_str_params(),
                                 indicators.EfficiencyRatio,
                                 self.start_date, self.end_date,
                                 window_size=10):
            self.eff_ratio.append(t)
            yield t

    def run(self):
        max_bars = 100
        self.target.makedirs()
        data = ut.input_df([self.turtle])
        garmp.normalize_time(data)
        data = data.tail(max_bars)
        # trades = ut.input_df([self.turtle])
        volume_ma = ut.input_df(self.vma)
        garmp.normalize_time(volume_ma)
        volume_ma = volume_ma.tail(max_bars)
        fast_ma = ut.input_df(self.fast_ma)
        garmp.normalize_time(fast_ma)
        fast_ma = fast_ma.tail(max_bars)
        slow_ma = ut.input_df(self.slow_ma)
        garmp.normalize_time(slow_ma)
        slow_ma = slow_ma.tail(max_bars)
        eff_ratio = ut.input_df(self.eff_ratio)
        garmp.normalize_time(eff_ratio)
        eff_ratio = eff_ratio.tail(max_bars)

        style.use("ggplot")
        ohlcv = data[["time", "open", "high", "low", "close", "volume"]].values
        fig, (ax1, ax2) = plt.subplots(2, sharex=True, figsize=(21, 9), dpi=120)
        title = "Turtle {exchange}: {pair} ({period})\n" \
                "(entry={entry}, exit={exit})".format(**self.to_str_params())
        plt.suptitle(title.upper(), horizontalalignment="left")
        with garmp.candles_and_volume(ax1, ohlcv, self.period) \
                as (c_ax, v_ax, shadows, rects):
            volume_ma.plot(ax=v_ax, kind="line", lw=1.0, color="dimgrey",
                           x="time", y=volume_ma.columns[0])
            # garmp.entries_and_exits(c_ax, data, rects)
            # garmp.trade_detail(c_ax, trades, ohlcv)
            garmp.turtle_signals(c_ax, data, self.entry, self.exit)
            fast_ma.plot(ax=c_ax, kind="line", lw=1.0, color="orange",
                         x="time", y=fast_ma.columns[0])
            slow_ma.plot(ax=c_ax, kind="line", lw=1.0, color="darkcyan",
                         x="time", y=slow_ma.columns[0])
            ax2.plot(eff_ratio.values[:, 1], eff_ratio.values[:, 0])
            ax2.yaxis.set_label_position("right")
            # ax2.legend().set_visible(False)
        plt.savefig(self.target.path, bboxinches="tight")
        plt.close("all")

    def output(self):
        if ut.ongoing_month(self.end_date):
            suffix = "TMP-{:%m-%d_%H}"
            suffix = suffix.format(ut.latest_full_period(self.period))
            path = os.path.join(self.destination_path, "strategies",
                                ut.task_filename(self, "png",
                                                 suffix=suffix,
                                                 exclude=["volume_window",
                                                          "price_fast_window",
                                                          "price_slow_window"]))
        else:
            path = os.path.join(self.destination_path, "strategies",
                                ut.task_filename(self, "png",
                                                 exclude=["volume_window",
                                                          "price_fast_window",
                                                          "price_slow_window"]))
        self.target = luigi.LocalTarget(path)
        yield self.target
