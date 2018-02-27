import matplotlib
matplotlib.use('Agg')

import chrysophylax.plotting as chp
import datetime
import indicators
import luigi
import matplotlib.dates as mdates
import matplotlib.patches as patches
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
from matplotlib.ticker import AutoMinorLocator
from matplotlib.finance import candlestick_ohlc



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
        chp.configure_date_axis(ax1, self.period)
        mx = np.max(ohlcv[:, -1])
        ax1.set_ylim(mx * -0.1, mx * 8)
        ax1.get_yaxis().set_visible(False)
        up = ohlcv[:, 1] <= ohlcv[:, 4]
        down = ohlcv[:, 1] > ohlcv[:, 4]
        ax1.bar(ohlcv[up, 0], ohlcv[up, -1], 0.6 / ut.PERIODS[self.period], color="g", alpha=0.5)
        ax1.bar(ohlcv[down, 0], ohlcv[down, -1],0.6 / ut.PERIODS[self.period], color="r", alpha=0.5)
        volume_ma.plot(ax=ax1, kind="line", lw=1.0, color="dimgrey",
                       x="time", y=volume_ma.columns[0])

        ax2 = ax1.twinx()
        ax2.tick_params(axis='both', which='major', labelsize=8)
        ax2.set_ylim(np.min(ohlcv[:, 3]) * 0.8, np.max(ohlcv[:, 2] * 1.2))
        shadows, rects = candlestick_ohlc(ax2, ohlcv, colorup="g", width=0.6 / ut.PERIODS[self.period])
        for rect, row in zip(rects, data.itertuples()):
            if row.entry_long == True:
                ax2.plot(rect.get_x(),
                         row.open,
                         ">", color="blue")
            if row.exit_long == True:
                ax2.plot(rect.get_x() + rect.get_width(),
                         row.open,
                         "<", color="blue")
            if row.entry_short == True:
                ax2.plot(rect.get_x(),
                         row.open,
                         ">", color="maroon")
            if row.exit_short == True:
                ax2.plot(rect.get_x() + rect.get_width(),
                         row.open,
                         "<", color="maroon")
        for row in trades.itertuples():
            y1 = min(row.entry_price, row.exit_price)
            height = abs(row.entry_price - row.exit_price)
            entry_time = mdates.date2num(pd.to_datetime(row.entry_time))
            exit_time = mdates.date2num(pd.to_datetime(row.exit_time))
            width = exit_time - entry_time
            ax2.add_patch(patches.Rectangle((entry_time, y1), width, height,
                                            facecolor="cyan", alpha=0.4))
        max_entry = "high_max_{}".format(self.entry)
        min_entry = "low_min_{}".format(self.entry)
        max_exit = "high_max_{}".format(self.exit)
        min_exit = "low_min_{}".format(self.exit)
        ax2.fill_between(data.index.values, data[min_exit].values,
                         data[max_entry].values, lw=0., alpha=0.2,
                         facecolor="chartreuse")
        ax2.fill_between(data.index.values, data[min_entry].values,
                         data[max_exit].values, lw=0., alpha=0.2,
                         facecolor="indianred")
        fast_ma.plot(ax=ax2, kind="line", lw=1.0, color="orange",
                     x="time", y=fast_ma.columns[0])
        slow_ma.plot(ax=ax2, kind="line", lw=1.0, color="darkcyan",
                     x="time", y=slow_ma.columns[0])
        ax1.legend().set_visible(False)
        ax2.legend().set_visible(False)
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
