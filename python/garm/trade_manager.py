import logging
import pandas as pd


def execute_strategy(parms, data):
    balance = parms.balance
    tm = TradeManager()
    label = "{}".format(parms.__class__.__name__).lower()
    data["time"] = data.index
    for row in data.itertuples():
        if not(parms.start_date <= row.Index.date() <= parms.end_date):
            continue
        balance += tm.update(row.time, row.open, row.atr_20)
        if row.exit_long == True and tm.open_idx["long"]:
            for t in tm.open:
                balance += tm.close_trade(t, label)
        if row.exit_short == True and tm.open_idx["short"]:
            for t in tm.open:
                balance += tm.close_trade(t, label)
        if balance < 1e-2:
            balance = 0.0
        if balance > 0.0:
            stop_loss = row.atr_20 * parms.stop_loss_multiplier
            if stop_loss <= 0:
                stop_loss = row.open
            # Do something about contract size
            if stop_loss / row.open > parms.balance_pctg:
                contracts = balance * parms.balance_pctg / stop_loss
            else:
                contracts = balance * parms.balance_pctg / stop_loss
            # print "balance", balance, contracts * row.open
            # print "trailing_stop", row.atr_20 * parms.trailing_stop_multiplier
            if row.entry_long == True and not tm.open_idx["short"] \
                                      and len(tm.open_idx["long"]) < 1 \
                                      and not parms.disable_longs:
                balance += tm.open_trade("long", contracts, stop_loss,
                                         parms.trailing_stop_multiplier, label)
            if row.entry_short == True and not tm.open_idx["long"] \
                                      and len(tm.open_idx["short"]) < 1 \
                                      and not parms.disable_shorts:
                balance += tm.open_trade("short", contracts, stop_loss,
                                         parms.trailing_stop_multiplier, label,
                                         short_leverage=parms.short_leverage)
        if row.open < row.close:
            balance += tm.update(row.time, row.low, row.atr_20)
            balance += tm.update(row.time, row.high, row.atr_20)
        else:
            balance += tm.update(row.time, row.high, row.atr_20)
            balance += tm.update(row.time, row.low, row.atr_20)
        balance += tm.update(row.time, row.close, row.atr_20)
    result = pd.DataFrame(tm.closed)
    return result


class TradeManager(object):
    def __init__(self):
        self.open = []
        self.open_idx = {"long": [], "short": []}
        self.closed = []
        self.time = None
        self.price = None

    def update(self, time, price, trailing_base):
        if self.price is not None and self.time > time:
            raise RuntimeError("Update time is in the past")
        self.time = time
        self.price = price
        result = 0
        self.trailing_base = trailing_base
        self.update_stats()
        result += self.check_stop_losses()
        result += self.check_trailing_stops()
        return result

    def update_stats(self):
        for t in self.open[:]:
            t["max_price"] = max(t["max_price"], self.price)
            t["min_price"] = min(t["min_price"], self.price)
            if t["direction"] == "long":
                t["max_drawdown"] = max(t["max_drawdown"],
                                        1.0 - (self.price / t["max_price"]))
            else:
                t["max_drawdown"] = max(t["max_drawdown"],
                                        (self.price - t["min_price"])
                                        / t["min_price"])

    def check_stop_losses(self):
        result = 0
        for t in self.open[:]:
            if t["direction"] == "long":
                stop_value = t["entry_price"] - t["stop_loss"]
                if self.price < stop_value:
                    result += self.close_trade(t, "stop_loss", stop_value)
            else:
                stop_value = t["entry_price"] + t["stop_loss"]
                if self.price > stop_value:
                    result += self.close_trade(t, "stop_loss", stop_value)
        return result

    def check_trailing_stops(self):
        result = 0
        for t in self.open[:]:
            if t["trailing_stop_multiplier"] > 0:
                stop_diff = self.trailing_base * t["trailing_stop_multiplier"]
                if t["direction"] == "long":
                    stop_value = t["max_price"] - stop_diff
                    if self.price > t["entry_price"] and self.price < stop_value:
                        print t["contracts"], stop_diff, self.price, stop_value
                        result += self.close_trade(t, "trailing_stop")
                elif self.price < t["entry_price"]:
                    stop_value = t["min_price"] + stop_diff
                    if self.price > stop_value:
                        result += self.close_trade(t, "trailing_stop")
        return result

    def open_trade(self, direction, contracts, stop_loss,
                   trailing_stop_multiplier, label, short_leverage=2.0):
        if direction == "long" and self.open_idx["short"]:
            raise RuntimeError("Cannot open long with outstanding "
                               "shorts")
        if direction == "short" and self.open_idx["long"]:
            raise RuntimeError("Cannot open short with outstanding "
                               "longs")
        trade = dict(direction=direction,
                     contracts=contracts,
                     entry_price=self.price, entry_time=self.time,
                     stop_loss=stop_loss,
                     trailing_stop_multiplier=trailing_stop_multiplier,
                     max_price=self.price, min_price=self.price,
                     max_drawdown=0.0,
                     entry_label=label,
                     short_leverage=0.0,
                     initial_margin=0.0, stop_loss_margin=0.0)
        if direction == "short":
            trade["short_leverage"] = short_leverage
            factor = trade["short_leverage"] / trade["entry_price"]
            trade["initial_margin"] = trade["entry_price"] * factor
            stop_exit_price = trade["entry_price"] - trade["stop_loss"]
            trade["stop_loss_margin"] = stop_exit_price * factor
            if trade["stop_loss_margin"] < 0.8:
                logging.warning("Not enough margin for trade")
                return 0
        self.open.append(trade)
        self.open_idx[direction].append(trade)
        if direction == "long":
            trade["entry_value"] = -trade["contracts"] * trade["entry_price"]
        else:
            trade["entry_value"] =  -trade["contracts"] * trade["entry_price"] \
                                    / trade["short_leverage"]
        return trade["entry_value"]

    def close_trade(self, trade, label, stop_value=None):
        trade["exit_price"] = self.price
        if stop_value is not None:
            trade["exit_price"] = stop_value
        trade["exit_time"] = self.time
        trade["exit_label"] = label
        price_delta = trade["exit_price"] - trade["entry_price"]
        if trade["direction"] == "short":
            price_delta *= -1
        trade["profit"] = trade["contracts"] * price_delta
        trade["r_multiple"] = price_delta /  trade["stop_loss"]
        trade["exit_value"] = trade["contracts"] * trade["entry_price"] + \
                              trade["profit"]
        if trade["direction"] == "short":
            trade["exit_value"] = trade["contracts"] * trade["entry_price"] \
                                   / trade["short_leverage"] + trade["profit"]
        self.open.remove(trade)
        self.open_idx[trade["direction"]].remove(trade)
        self.closed.append(trade)
        return trade["exit_value"]
