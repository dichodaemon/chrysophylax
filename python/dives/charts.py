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
from matplotlib.ticker import AutoMinorLocator
from matplotlib.finance import candlestick_ohlc



@inherits(strategies.SimpleTurtle)
class TurtlePlot(luigi.Task):
    volume_window = luigi.IntParameter(default=30)

    def requires(self):
        params = self.to_str_params()
        self.turtle = strategies.SimpleTurtle.from_str_params(params)
        yield self.turtle
        vma_params = self.to_str_params()
        vma_params["window_size"] = self.volume_window
        # self.vma = VolumeMovingAverage.from_str_params(vma_params)
        # yield self.vma

    def run(self):
        style.use("ggplot")
        data = pd.read_csv(self.turtle.requires().target.path,
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
        chp.configure_date_axis(ax1, self.period)
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
        plt.savefig(self.target.path, bboxinches="tight")
        if self.rerun is not None:
            self.rerun.done()

    def output(self):
        path = os.path.join(self.destination_path,
                            ut.task_filename(self, "png",
                                             exclude=["destination_path",
                                                      "volume_window"]))
        self.target = luigi.LocalTarget(path)
        yield self.target
        self.rerun = RunAnywayTarget(self)
        yield self.rerun
