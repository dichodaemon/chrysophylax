import matplotlib.dates as mdates
import matplotlib.patches as patches
import numpy as np
import pandas as pd

from contextlib import contextmanager
from mpl_finance import candlestick_ohlc

PERIODS = {
    "1d": 1,
    "1h": 24,
    "4h": 6,
    "6h": 4,
    "8h": 3
}

def normalize_time(df):
    df["time"] = pd.to_datetime(df.index)
    df["time"] = df["time"].apply(lambda d: mdates.date2num(d.to_pydatetime()))


def configure_date_axis(axis, period):
    axis.tick_params(axis='y', which='major', labelsize=10)
    axis.tick_params(axis='x', which='major', labelsize=9, labelcolor="k",
                     pad=10)
    axis.tick_params(axis='x', which='minor', labelsize=8, labelcolor="k")
    axis.xaxis_date()
    if period == "1d":
        max_locator = mdates.WeekdayLocator(byweekday=0)
        max_formatter = mdates.DateFormatter("%m-%d-%Y")
        min_locator = mdates.WeekdayLocator(byweekday=[3])
        min_formatter = mdates.DateFormatter("%d")
    elif period == "1h":
        max_locator = mdates.DayLocator()
        max_formatter = mdates.DateFormatter("%m-%d\n%Y")
        min_locator = mdates.HourLocator(byhour=[6, 12, 18])
        min_formatter = mdates.DateFormatter("%H:00")
    elif period == "4h":
        max_locator = mdates.DayLocator()
        max_formatter = mdates.DateFormatter("%m-%d\n%Y")
        min_locator = mdates.HourLocator(byhour=[12])
        min_formatter = mdates.DateFormatter("%H:00")
    axis.xaxis.set_major_locator(max_locator)
    axis.xaxis.set_major_formatter(max_formatter)
    axis.xaxis.set_minor_locator(min_locator)
    axis.xaxis.set_minor_formatter(min_formatter)

@contextmanager
def candles_and_volume(ax, ohlcv, period):
    configure_date_axis(ax, period)
    mx = np.max(ohlcv[:, -1])
    ax.set_ylim(mx * -0.1, mx * 8)
    up = ohlcv[:, 1] <= ohlcv[:, 4]
    down = ohlcv[:, 1] > ohlcv[:, 4]
    ax.bar(ohlcv[up, 0], ohlcv[up, -1], 0.6 / PERIODS[period],
           color="g", alpha=0.5)
    ax.bar(ohlcv[down, 0], ohlcv[down, -1],0.6 / PERIODS[period],
           color="r", alpha=0.5)
    cax = ax.twinx()
    cax.tick_params(axis='both', which='major', labelsize=8)
    cax.set_ylim(np.min(ohlcv[:, 3]) * 0.8, np.max(ohlcv[:, 2] * 1.2))
    shadows, rects = candlestick_ohlc(cax, ohlcv, colorup="g",
                                      width=0.6 / PERIODS[period])
    yield cax, ax, shadows, rects
    ax.yaxis.set_visible(False)
    ax.legend().set_visible(False)
    cax.legend().set_visible(False)

def entries_and_exits(ax, signals, ohlcv_rects):
    for rect, row in zip(ohlcv_rects, signals.itertuples()):
        if row.entry_long == True:
            ax.plot(rect.get_x(),
                     row.open,
                     ">", color="blue")
        if row.exit_long == True:
            ax.plot(rect.get_x() + rect.get_width(),
                     row.open,
                     "<", color="blue")
        if row.entry_short == True:
            ax.plot(rect.get_x(),
                     row.open,
                     ">", color="maroon")
        if row.exit_short == True:
            ax.plot(rect.get_x() + rect.get_width(),
                     row.open,
                     "<", color="maroon")

def trade_detail(ax, trades, ohlcv):
    start_t = ohlcv[:, 0].min()
    end_t = ohlcv[:, 0].max()
    for row in trades.itertuples():
        y1 = min(row.entry_price, row.exit_price)
        height = abs(row.entry_price - row.exit_price)
        entry_time = mdates.date2num(pd.to_datetime(row.entry_time))
        exit_time = mdates.date2num(pd.to_datetime(row.exit_time))
        if start_t <= entry_time <= end_t or start_t <= exit_time <= end_t:
            entry_time = max(start_t, entry_time)
            exit_time = min(end_t, exit_time)
            width = exit_time - entry_time
            ax.add_patch(patches.Rectangle((entry_time, y1), width, height,
                                           facecolor="cyan", alpha=0.4))
def turtle_signals(ax, signals, entry, exit):
        ax.fill_between(signals.index.values,
                        signals["long_entry_value"].values,
                        signals["long_exit_value"].values,
                        lw=0., alpha=0.3, facecolor="chartreuse")
        ax.fill_between(signals.index.values,
                        signals["short_entry_value"].values,
                        signals["short_exit_value"].values,
                        lw=0., alpha=0.3, facecolor="indianred")
