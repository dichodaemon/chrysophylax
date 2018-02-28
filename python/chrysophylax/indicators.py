import pandas as pd

from functools import reduce


def max_in_window(parms, data):
    return data[parms.input_column].shift().rolling(parms.window_size).max()

def min_in_window(parms, data):
    return data[parms.input_column].shift().rolling(parms.window_size).min()

def moving_average(parms, data):
    return data[parms.input_column].rolling(parms.window_size).mean()

def true_range(parms, data):
    data["hl"] = data["high"] - data["low"]
    data["hpdc"] = data["high"] - data.close.shift()
    data["pdcl"] = data.close.shift() - data["low"]
    return data[["hl", "hpdc", "pdcl"]].max(axis=1)

def average_true_range(parms, data):
    return data["true_range"].ewm(span=parms.window_size, adjust=False).mean()


def turtle_prepare_signals(parms, data):
    max_entry = "high_max_{}".format(parms.entry)
    min_entry = "low_min_{}".format(parms.entry)
    max_exit = "high_max_{}".format(parms.exit)
    min_exit = "low_min_{}".format(parms.exit)
    data["entry_long"] = (data[max_entry].shift()
                               < data[max_entry])
    data["entry_short"] = (data[min_entry].shift()
                                > data[min_entry])
    data["exit_long"] = (data[min_exit].shift() >
                              data[min_exit])
    data["exit_short"] = (data[max_exit].shift() <
                               data[max_exit])

def buy_and_hold_prepare_signals(parms, data):
    if row.Index.date() == parms.start_date:
        data["entry_long"] = True
    if row.Index.date() == parms.end_date:
        date["exit_long"] = False
