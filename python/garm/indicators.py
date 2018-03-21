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


def simple_turtle_signals(parms, data):
    max_entry = "high_max_{}".format(parms.entry)
    min_entry = "low_min_{}".format(parms.entry)
    max_exit = "high_max_{}".format(parms.exit)
    min_exit = "low_min_{}".format(parms.exit)
    atr = "atr_{}".format(parms.atr_window)
    data["stop_loss_delta"] = data[atr] * parms.stop_loss_multiplier
    data["trailing_stop_delta"] = data[atr] * parms.trailing_stop_multiplier
    data["long_setup"] = "True"
    data["long_entry"] = "ticker.price > trigger.{}".format(max_entry)
    data["long_exit"] = "ticker.price < trigger.{}".format(min_exit)
    data["short_setup"] = "True"
    data["short_entry"] = "ticker.price < trigger.{}".format(min_entry)
    data["short_exit"] = "ticker.price > trigger.{}".format(max_exit)


def turtle_soup_signals(parms, data):
    min_column = "low_min_{}".format(parms.entry_window)
    atr_column = "atr_{}".format(parms.atr_window)
    data["setup"] = data[min_column] < data.shift()[min_column]
    data["setup"] = (data["setup"] == True) \
                  & (data["setup"].shift().rolling(parms.wait).max() == False)
    data["long_entry_value"] = float("nan")
    data["long_entry_type"] = "disabled"
    selected = (data["setup"] == True)
    data["long_entry_value"][selected] = \
            data[min_column][selected] \
            + data[atr_column][selected] * parms.entry_multiplier
    data["long_entry_type"][selected] = "price_gt"
    data["long_exit_value"] = float("nan")
    data["long_exit_type"] = "disabled"
    data["short_entry_value"] = float("nan")
    data["short_entry_type"] = "disabled"
    data["short_exit_value"] = float("nan")
    data["short_exit_type"] = "disabled"
    data["stop_loss_delta"] = data[atr_column] * \
                              (parms.entry_multiplier + parms.exit_multiplier)
    data["trailing_stop_delta"] = data[atr_column] \
                                  * parms.trailing_stop_multiplier


def buy_and_hold_signals(parms, data):
    if row.Index.date() == parms.start_date:
        data["entry_long"] = True
    if row.Index.date() == parms.end_date:
        date["exit_long"] = False
