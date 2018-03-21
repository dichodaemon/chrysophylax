import pandas as pd


class Trader(object):
    FIELD_NAMES = [
        "pair",
        "exchange",
        "period",
        "direction",
        "contracts",
        "entry_time",
        "entry_price",
        "exit_time",
        "exit_price",
        "reason",
        "profit",
        "cumm_profit",
        "max_drawdown",
        "entry_r",
        "pr_ratio",
        "r_profit",
        "entry_value",
        "exit_value",
        "last_price",
        "max_price",
        "min_price",
    ]
    def __init__(self, balance, risk_percentage, pyramiding=1, allow_longs=True,
                 allow_shorts=True):
        self.balance = balance
        self.risk_percentage = risk_percentage
        self.pyramiding = pyramiding
        self.allow_longs = allow_longs
        self.allow_shorts = allow_shorts
        self.open = {}
        self.closed = []

    def close_trades(self, trade_key, signal_row):
        if trade_key not in self.open:
            return []
        result = []
        for prefix in ["long_exit", "short_exit"]:
            signal = getattr(signal_row, "{}_signal".format(prefix))
            direction = prefix.split("_")[0]
            if signal == True:
                trades = self.open[trade_key]
                for t in trades[:]:
                    if t["direction"] == direction:
                        t["reason"] = "exit_flag"
                        result.append(trades.pop(0))
        trades = self.open[trade_key]
        for t in trades[:]:
            stopped = False
            reason = "stop_loss"
            if t["direction"] == "long":
                sl_value = t["entry_price"] - signal_row.stop_loss_delta
                ts_value = t["max_price"] - signal_row.trailing_stop_delta
                stopped = signal_row.price < sl_value
                if not stopped:
                    stopped = signal_row.price > t["entry_price"] \
                              and signal_row.price < ts_value
                    reason = "trailing_stop"
            else:
                sl_value = t["entry_price"] + signal_row.stop_loss_delta
                ts_value = t["min_price"] + signal_row.trailing_stop_delta
                stopped = signal_row.price > sl_value
                if not stopped:
                    stopped = signal_row.price < t["entry_price"] \
                              and signal_row.price > ts_value
                    reason = "trailing_stop"
            if stopped:
                t["reason"] = reason
                result.append(trades.pop(0))
        if not self.open[trade_key]:
            del self.open[trade_key]
            self.closed.extend(result)
            for t in result:
                t["exit_time"] = signal_row.time
                t["exit_price"] = signal_row.price
                t["exit_value"] = t["contracts"] * t["exit_price"]
                t["profit"] = t["exit_value"] - t["entry_value"]
                t["r_profit"] = (t["exit_price"] - t["entry_price"])
                t["r_profit"] /= t["entry_r"]
                if t["direction"] == "short":
                    t["r_profit"] *= -1
                    t["profit"] *= -1
                self.balance += t["entry_value"] + t["profit"]
                t["cumm_profit"] = self.balance
            return result
        return []

    def open_trades(self, trade_key, signal_row):
        direction = None
        if trade_key in self.open:
            if len(self.open) >= self.pyramiding:
                return []
            direction = self.open[trade_key]["direction"]
        for prefix in ["long_entry", "short_entry"]:
            signal = getattr(signal_row, "{}_signal".format(prefix))
            if not (signal == True):
                continue
            signal_direction = prefix.split("_")[0]
            if direction is not None and direction != signal_direction:
                continue
            pr_ratio = signal_row.price / signal_row.stop_loss_delta
            entry_value = self.balance * self.risk_percentage * pr_ratio
            contracts = entry_value / signal_row.price
            if entry_value > self.balance:
                break
            self.balance -= entry_value
            trade = dict(exchange=signal_row.exchange,
                         pair=signal_row.pair,
                         period=signal_row.period,
                         direction=signal_direction,
                         entry_time=signal_row.time,
                         entry_price=signal_row.price,
                         contracts=contracts,
                         entry_value=entry_value,
                         pr_ratio=pr_ratio,
                         last_price=signal_row.price,
                         max_price=signal_row.price,
                         min_price=signal_row.price,
                         max_drawdown=0.0,
                         entry_r=signal_row.stop_loss_delta)
            if trade_key not in self.open:
                self.open[trade_key] = []
            self.open[trade_key].append(trade)
            return [trade]
        return []

    def update(self, signal_row):
        trade_key = "{}-{}-{}".format(signal_row.exchange, signal_row.pair,
                                      signal_row.period)
        if trade_key in self.open:
            for trade in self.open[trade_key]:
                trade["last_price"] = signal_row.price
                trade["max_price"] = max(signal_row.price, trade["max_price"])
                trade["min_price"] = min(signal_row.price, trade["min_price"])
                drawdown = 1.0 - signal_row.price / trade["max_price"]
                trade["max_drawdown"] = max(trade["max_drawdown"], drawdown)
        closed = self.close_trades(trade_key, signal_row)
        opened = self.open_trades(trade_key, signal_row)
        return opened, closed
