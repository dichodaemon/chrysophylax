import pandas as pd

from functools import reduce


def max_in_window(parms, data):
    return data["high"].shift().rolling(parms.window_size).max()

def min_in_window(parms, data):
    return data["low"].shift().rolling(parms.window_size).min()

def volume_ma(parms, data):
    return data["volume"].rolling(parms.window_size).mean()

def true_range(parms, data):
    data["hl"] = data["high"] - data["low"]
    data["hpdc"] = data["high"] - data.close.shift()
    data["pdcl"] = data.close.shift() - data["low"]
    return data[["hl", "hpdc", "pdcl"]].max(axis=1)

def average_true_range(parms, data):
    return data["true_range"].ewm(span=parms.window_size, adjust=False).mean()

def trade_step(date, price, balance,
               pyramiding, percentage,
               stop_loss, trailing_stop,
               entry_longs, exit_longs,
               entry_shorts, exit_shorts,
               open_longs, open_shorts,
               closed, allow_shorts):
    for open_trades, signals in [(open_longs, exit_longs),
                                 (open_shorts, exit_shorts)]:
        for trades in open_trades.values():
            for trade in list(trades):
                if trade["trailing_stop"] > 0:
                    trade["trailing_stop"] = trailing_stop
                    if (trade["type"] == "long") and \
                       (price > trade["trailing_price"]):
                       trade["trailing_price"] = price
                    if (trade["type"] == "short") and \
                       (price < trade["trailing_price"]):
                       trade["trailing_price"] = price
                is_a_stop = False
                is_a_trailing_stop = False
                if signals:
                    trades.remove(trade)
                elif trade["type"] == "long" \
                     and trade["stop_loss"] > 0 \
                     and price - trade["entry_price"] < -trade["stop_loss"]:
                    is_a_stop = True
                    trades.remove(trade)
                elif trade["type"] == "short" \
                     and trade["stop_loss"] > 0 \
                     and price - trade["entry_price"] > trade["stop_loss"]:
                    is_a_stop = True
                    trades.remove(trade)
                elif trade["type"] == "long" \
                     and trade["trailing_stop"] > 0 \
                     and price - trade["trailing_price"] < -trade["trailing_stop"]:
                    is_a_trailing_stop = True
                    trades.remove(trade)
                elif trade["type"] == "short" \
                     and trade["trailing_stop"] > 0 \
                     and price - trade["trailing_price"] > trade["trailing_stop"]:
                    is_a_trailing_stop = True
                    trades.remove(trade)
                else:
                    continue
                trade["exit_price"] = price
                trade["exit_time"] = date
                if trade["type"] == "long":
                    trade["profit"] = (trade["exit_price"] - trade["entry_price"]) \
                                      * trade["contracts"]
                    balance += trade["exit_price"] * trade["contracts"]
                    if is_a_stop:
                        trade["exit_longs"] = "stop_loss"
                    elif is_a_trailing_stop:
                        trade["exit_longs"] = "trailing_stop"
                    else:
                        trade["exit_longs"] = ",".join(exit_longs)
                else:
                    trade["profit"] = -(trade["exit_price"] - trade["entry_price"]) \
                                      * trade["contracts"]
                    balance += trade["profit"]
                    if is_a_stop:
                        trade["exit_shorts"] = "stop_loss"
                    elif is_a_trailing_stop:
                        trade["exit_shorts"] = "trailing_stop"
                    else:
                        trade["exit_shorts"] = ",".join(exit_shorts)
                closed.append(trade)
    outstanding_longs = sum([len(x) for x in open_longs.values()])
    outstanding_shorts = sum([len(x) for x in open_shorts.values()])
    assert(outstanding_longs == 0 or outstanding_shorts == 0)
    if outstanding_shorts == 0 and entry_longs:
        for label in entry_longs:
            if not label in open_longs:
                open_longs[label] = []
            if outstanding_longs < pyramiding:
                trade_amount = balance * percentage
                trade = {
                    "type": "long",
                    "label": label,
                    "entry_price": price,
                    "trailing_price": 0,
                    "stop_loss": stop_loss,
                    "trailing_stop": trailing_stop,
                    "entry_time": date,
                    "contracts": trade_amount / price,
                    "entry_longs": ",".join(entry_longs),
                    "entry_shorts": ",".join(entry_shorts)
                }
                if trade["trailing_stop"] > 0:
                    trade["trailing_price"] = price
                open_longs[label].append(trade)
                balance -= trade_amount
                outstanding_longs += 1
    if outstanding_longs == 0 and entry_shorts and allow_shorts:
        for label in entry_shorts:
            if not label in open_shorts:
                open_shorts[label] = []
            if outstanding_shorts < pyramiding:
                trade_amount = balance * percentage
                trade = {
                    "type": "short",
                    "label": label,
                    "entry_price": price,
                    "trailing_price": 0,
                    "stop_loss": stop_loss,
                    "trailing_stop": trailing_stop,
                    "entry_time": date,
                    "contracts": trade_amount / price,
                    "entry_longs": ",".join(entry_longs),
                    "entry_shorts": ",".join(entry_shorts)
                }
                if trade["trailing_stop"] > 0:
                    trade["trailing_price"] = price
                open_shorts[label].append(trade)
                outstanding_shorts += 1
    return balance

def turtle_collect_signals(parms, row):
    entry_longs = []
    if row.entry_long:
        entry_longs.append("turtle")
    entry_shorts = []
    if row.entry_short:
        entry_shorts.append("turtle")
    exit_longs = []
    if row.exit_long:
        exit_longs.append("turtle")
    exit_shorts = []
    if row.exit_short:
        exit_shorts.append("turtle")
    return entry_longs, exit_longs, entry_shorts, exit_shorts, \
           row.atr_20 * parms.stop_loss_multiplier, \
           row.atr_20 * parms.trailing_stop_multiplier


def turtle_prepare_signals(parms, data):
    max_entry = "max_{}".format(parms.entry)
    min_entry = "min_{}".format(parms.entry)
    max_exit = "max_{}".format(parms.exit)
    min_exit = "min_{}".format(parms.exit)
    data["entry_long"] = (data[max_entry].shift()
                               < data[max_entry])
    data["entry_short"] = (data[min_entry].shift()
                                > data[min_entry])
    data["exit_long"] = (data[min_exit].shift() >
                              data[min_exit])
    data["exit_short"] = (data[max_exit].shift() <
                               data[max_exit])

def buy_and_hold_prepare_signals(parms, data):
    pass

def buy_and_hold_collect_signals(parms, row):
    entry_longs = []
    exit_longs = []
    entry_shorts = []
    exit_shorts = []
    if row.Index.date() == parms.start_date:
        entry_longs = ["buy"]
    if row.Index.date() == parms.end_date:
        exit_longs = ["sell"]
    return entry_longs, exit_longs, entry_shorts, exit_shorts, 0, 0

def execute_strategy(parms, data, collect_signals):
    balance = parms.balance
    open_longs = {}
    open_shorts = {}
    closed = []
    for row in data.itertuples():
        if not(parms.start_date <= row.Index.date() <= parms.end_date):
            continue
        entry_longs, exit_longs, entry_shorts, exit_shorts, \
        stop_loss, trailing_stop = collect_signals(row)
        balance = trade_step(row.Index.date(), row.open, balance,
                             parms.pyramiding,
                             parms.max_trade_percentage,
                             stop_loss, trailing_stop,
                             entry_longs, exit_longs,
                             entry_shorts, exit_shorts,
                             open_longs, open_shorts,
                             closed, parms.allow_shorts)
        if balance < 0.:
            break
    result = pd.DataFrame(closed)
    return result
