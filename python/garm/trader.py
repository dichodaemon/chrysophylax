import pandas as pd


class Trader(object):
    def __init__(self, pyramiding=1, allow_longs=True, allow_shorts=True):
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
                        t["exit_price"] = signal_row.price
                        t["exit_time"] = signal_row.time
                        result.append(trades.pop(0))
        if not self.open[trade_key]:
            del self.open[trade_key]
            self.closed.extend(result)
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
            trade = dict(exchange=signal_row.exchange,
                         pair=signal_row.pair,
                         period=signal_row.period,
                         direction=signal_direction,
                         entry_time=signal_row.time,
                         entry_price=signal_row.price)
            if trade_key not in self.open:
                self.open[trade_key] = []
            self.open[trade_key].append(trade)
            return [trade]
        return []

    def update(self, signal_row):
        trade_key = "{}-{}-{}".format(signal_row.exchange, signal_row.pair,
                                      signal_row.period)
        closed = self.close_trades(trade_key, signal_row)
        opened = self.open_trades(trade_key, signal_row)
        return opened, closed
