import pandas as pd


def execute_strategy(parms, data):
    balance = parms.balance
    tm = TradeManager()
    label = "{}".format(parms.__class__.__name__).lower()
    data["time"] = data.index
    for row in data.itertuples():
        if not(parms.start_date <= row.Index.date() <= parms.end_date):
            continue
        tm.update(row, row.atr_20)
        stop_loss = row.atr_20 * parms.stop_loss_multiplier
        if stop_loss <= 0:
            stop_loss = balance
        if row.exit_long == True and tm.open_idx["long"]:
            for t in tm.open:
                tm.close_trade(t, label)
                balance += t["profit"]
        if row.exit_short == True and tm.open_idx["short"]:
            for t in tm.open:
                tm.close_trade(t, label)
                balance += t["profit"]
        if balance > 0.0:
            if row.entry_long == True and not tm.open_idx["short"]:
                tm.open_trade("long", balance / row.close, stop_loss,
                              parms.trailing_stop_multiplier, label)
                balance = 0.0
            if row.entry_short == True and not tm.open_idx["long"]:
                tm.open_trade("short", balance / row.close, stop_loss,
                              parms.trailing_stop_multiplier, label)
                balance = 0.0
    result = pd.DataFrame(tm.closed)
    return result


class TradeManager(object):
    def __init__(self):
        self.open = []
        self.open_idx = {"long": [], "short": []}
        self.closed = []
        self.ohlc = None

    def update(self, ohlc, trailing_base):
        if self.ohlc is not None and self.ohlc.time >= ohlc.time:
            raise RuntimeError("Update time is in the past")
        self.ohlc = ohlc
        self.trailing_base = trailing_base
        self.check_stop_losses()
        self.check_trailing_stops()
        self.update_stats()

    def check_stop_losses(self):
        for t in self.open[:]:
            if t["direction"] == "long":
                stop_value = t["entry_price"] - t["stop_loss"]
                if self.ohlc.close < stop_value:
                    self.close_trade(t, "stop_loss")
            else:
                stop_value = t["entry_price"] + t["stop_loss"]
                if self.ohlc.close > stop_value:
                    self.close_trade(t, "stop_loss")

    def check_trailing_stops(self):
        for t in self.open[:]:
            if t["trailing_stop_multiplier"] >= 0:
                stop_diff = self.trailing_base * t["trailing_stop_multiplier"]
                if t["direction"] == "long":
                    stop_value = t["max_price"] - stop_diff
                    if self.ohlc.close < stop_value:
                        self.close_trade(t, "trailing_stop")
                else:
                    stop_value = t["min_price"] + stop_diff
                    if self.ohlc.close > stop_value:
                        self.close_trade(t, "trailing_stop")

    def update_stats(self):
        for t in self.open[:]:
            if t["direction"] == "long":
                t["max_drawdown"] = max(t["max_drawdown"],
                                        1.0 - (t["max_price"] / self.ohlc.low))
            else:
                t["max_drawdown"] = max(t["max_drawdown"],
                                        (t["min_price"] / self.ohlc.high) - 1.0)
            t["max_price"] = max(t["max_price"], self.ohlc.high)
            t["low_price"] = min(t["min_price"], self.ohlc.low)

    def open_trade(self, direction, contracts, stop_loss,
                   trailing_stop_multiplier, label):
        if direction == "long" and self.open_idx["short"]:
            raise RuntimeError("Cannot open long with outstanding "
                               "shorts")
        if direction == "short" and self.open_idx["long"]:
            raise RuntimeError("Cannot open short with outstanding "
                               "longs")
        trade = dict(direction=direction,
                     contracts=contracts,
                     entry_price=self.ohlc.open, entry_time=self.ohlc.time,
                     stop_loss=stop_loss,
                     trailing_stop_multiplier=trailing_stop_multiplier,
                     max_price=self.ohlc.close, min_price=self.ohlc.close,
                     max_drawdown=0.0,
                     entry_label=label)
        self.open.append(trade)
        self.open_idx[direction].append(trade)

    def close_trade(self, trade, label):
        trade["exit_price"] = self.ohlc.open
        trade["exit_time"] = self.ohlc.time
        trade["exit_label"] = label
        price_delta = trade["exit_price"] - trade["entry_price"]
        if trade["direction"] == "short":
            price_delta *= -1
        trade["profit"] = trade["contracts"] * price_delta
        trade["r_multiple"] = price_delta /  trade["stop_loss"]
        self.open.remove(trade)
        self.open_idx[trade["direction"]].remove(trade)
        self.closed.append(trade)

