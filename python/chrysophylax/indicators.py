from functools import reduce


def max_in_window(parms, data):
    return data["high"].shift().rolling(parms.window_size).max()

def min_in_window(parms, data):
    return data["low"].shift().rolling(parms.window_size).min()

def true_range(parms, data):
    data["hl"] = data["high"] - data["low"]
    data["hpdc"] = data["high"] - data.close.shift()
    data["pdcl"] = data.close.shift() - data["low"]
    return data[["hl", "hpdc", "pdcl"]].max(axis=1)

def average_true_range(parms, data):
    return data["true_range"].rolling(parms.window_size).mean()

def compute_turtle_step(date, price, balance,
                        pyramiding, percentage,
                        entry_longs, exit_longs,
                        entry_shorts, exit_shorts,
                        open_longs, open_shorts,
                        closed):
    for open_trades in [open_longs, open_shorts]:
        for label in open_trades:
            if label in open_trades:
                if open_trades[label]:
                    trade = open_trades[label].pop(-1)
                    trade["exit_price"] = price
                    trade["exit_time"] = date
                    trade["profit"] = (trade["exit_price"] - trade["entry_price"]) \
                                      * trade["contracts"]
                    balance += trade["profit"]
    outstanding_longs = reduce((lambda x, y: len(x) + len(y)), open_longs.values(), 0)
    outstanding_shorts = reduce((lambda x, y: len(x) + len(y)), open_shorts.values(), 0)
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
                    "entry_time": date,
                    "contracts": trade_amount / price,
                    "profit": -trade_amount
                }
                open_longs[label].append(trade)
                balance += trade["profit"]
                outstanding_longs += 1
    if outstanding_longs == 0 and entry_shorts:
        for label in entry_shorts:
            if not label in open_shorts:
                open_shorts[label] = []
            if outstanding_shorts < pyramiding:
                trade_amount = balance * percentage
                trade = {
                    "type": "long",
                    "label": label,
                    "entry_price": price,
                    "entry_time": date,
                    "contracts": -trade_amount / price,
                    "profit": 0
                }
                open_shorts[label].append(trade)
                balance += trade["profit"]
                outstanding_shorts += 1
    return balance


def simple_turtle(parms, data):
    data["slow_entry_long"] = (data["high"] > data["max_55"])
    data["slow_entry_short"] = (data["low"] < data["min_55"])
    data["slow_exit_long"] = (data["low"] < data["min_20"])
    data["slow_exit_short"] = (data["high"] > data["max_20"])
    data["fast_entry_long"] = (data["high"] > data["max_20"])
    data["fast_entry_short"] = (data["low"] < data["min_20"])
    data["fast_exit_long"] = (data["low"] < data["min_10"])
    data["fast_exit_short"] = (data["high"] > data["max_10"])
    data["in_long"] = False
    data["in_short"] = False
    data["entry_long_label"] = ""
    data["entry_short_label"] = ""
    data["exit_long"] = 0.0
    data["exit_long_label"] = ""
    data["exit_short"] = 0.0
    data["exit_short_label"] = ""
    balance = parms.balance
    open_longs = {}
    open_shorts = {}
    closed = []
    for row in data.itertuples():
        if not(parms.start_date <= row.Index.date() <= parms.end_date):
            continue
        entry_longs = []
        if row.fast_entry_long:
            entry_longs.append("fast")
        if row.slow_entry_long:
            entry_longs.append("slow")
        entry_shorts = []
        if row.fast_entry_short:
            entry_shorts.append("fast")
        if row.slow_entry_short:
            entry_shorts.append("slow")
        exit_longs = []
        if row.fast_exit_long:
            exit_longs.append("fast")
        if row.slow_exit_long:
            exit_longs.append("slow")
        exit_shorts = []
        if row.fast_exit_short:
            exit_shorts.append("fast")
        if row.slow_exit_short:
            exit_shorts.append("slow")
        compute_turtle_step(row.Index.date(), row.close, balance,
                            parms.pyramiding, parms.max_trade_percentage,
                            entry_longs, exit_longs,
                            entry_shorts, exit_shorts,
                            open_longs, open_shorts,
                            closed)
    result = pd.DataFrame(closed)
    return result
