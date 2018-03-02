import matplotlib
matplotlib.use('Agg')

import chrysophylax.plotting as chp
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
import utility as ut


from luigi.util import inherits
from luigi.contrib.simulate import RunAnywayTarget
from matplotlib import style



@inherits(strategies.SimpleTurtle)
class TurtlePlot(luigi.Task):
    volume_window = luigi.IntParameter(default=30)
    price_fast_window = luigi.IntParameter(default=30)
    price_slow_window = luigi.IntParameter(default=50)

    def requires(self):
        params = self.to_str_params()
        self.turtle = strategies.SimpleTurtle.from_str_params(params)
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

    def run(self):
        self.target.makedirs()
        data = ut.input_df([self.turtle.requires()])
        chp.normalize_time(data)
        data = data.tail(100)
        trades = ut.input_df([self.turtle])
        volume_ma = ut.input_df(self.vma)
        chp.normalize_time(volume_ma)
        volume_ma = volume_ma.tail(100)
        fast_ma = ut.input_df(self.fast_ma)
        chp.normalize_time(fast_ma)
        fast_ma = fast_ma.tail(100)
        slow_ma = ut.input_df(self.slow_ma)
        chp.normalize_time(slow_ma)
        slow_ma = slow_ma.tail(100)

        style.use("ggplot")
        ohlcv = data[["time", "open", "high", "low", "close", "volume"]].values
        fig, ax1 = plt.subplots(figsize=(16, 9), dpi=120)
        title = "Turtle {exchange}: {pair} ({period})\n" \
                "(entry={entry}, exit={exit})".format(**self.to_str_params())
        plt.title(title.upper(), loc="left")
        with chp.candles_and_volume(ax1, ohlcv, self.period) \
                as (c_ax, v_ax, shadows, rects):
            volume_ma.plot(ax=v_ax, kind="line", lw=1.0, color="dimgrey",
                           x="time", y=volume_ma.columns[0])
            chp.entries_and_exits(c_ax, data, rects)
            chp.trade_detail(c_ax, trades, ohlcv)
            chp.turtle_signals(c_ax, data, self.entry, self.exit)
            fast_ma.plot(ax=c_ax, kind="line", lw=1.0, color="orange",
                         x="time", y=fast_ma.columns[0])
            slow_ma.plot(ax=c_ax, kind="line", lw=1.0, color="darkcyan",
                         x="time", y=slow_ma.columns[0])
        plt.savefig(self.target.path, bboxinches="tight")
        plt.close("all")
        if self.rerun is not None:
            self.rerun.done()

    def output(self):
        path = os.path.join(self.destination_path, "strategies",
                            ut.task_filename(self, "png",
                                             exclude=["volume_window",
                                                      "price_fast_window",
                                                      "price_slow_window"]))
        self.target = luigi.LocalTarget(path)
        yield self.target
        self.rerun = RunAnywayTarget(self)
        yield self.rerun
