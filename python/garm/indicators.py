import pandas as pd

from functools import reduce


def max_in_window(parms, data):
    return data[parms.input_column].rolling(parms.window_size).max()

def min_in_window(parms, data):
    return data[parms.input_column].rolling(parms.window_size).min()

def moving_average(parms, data):
    return data[parms.input_column].rolling(parms.window_size).mean()

def true_range(parms, data):
    data["hl"] = data["high"] - data["low"]
    data["hpdc"] = data["high"] - data.close.shift()
    data["pdcl"] = data.close.shift() - data["low"]
    return data[["hl", "hpdc", "pdcl"]].max(axis=1)

def average_true_range(parms, data):
    return data["true_range"].rolling(parms.window_size).mean()

def efficiency_ratio(parms, data):
    movement_speed = (data.close
                    - data.shift(parms.window_size).close).abs()
    tmp = (data.close - data.shift().close).abs()
    volatility = tmp.rolling(parms.window_size).sum()
    result = movement_speed / volatility
    return result


def turtle_prepare_signals(parms, data):
    max_entry = "high_max_{}".format(parms.entry)
    min_entry = "low_min_{}".format(parms.entry)
    max_exit = "high_max_{}".format(parms.exit)
    min_exit = "low_min_{}".format(parms.exit)
    atr = "atr_{}".format(parms.atr_window)
    data["long_entry_value"] = data[max_entry]
    data["long_entry_type"] = "price_gt"
    data["long_exit_value"] = data[min_exit]
    data["long_exit_type"] = "price_lt"
    data["short_entry_value"] = data[min_entry]
    data["short_entry_type"] = "price_lt"
    data["short_exit_value"] = data[max_exit]
    data["short_exit_type"] = "price_gt"
    data["stop_loss_delta"] = data[atr] * parms.stop_loss_multiplier
    data["trailing_stop_delta"] = data[atr] * parms.trailing_stop_multiplier


def buy_and_hold_prepare_signals(parms, data):
    if row.Index.date() == parms.start_date:
        data["entry_long"] = True
    if row.Index.date() == parms.end_date:
        date["exit_long"] = False
