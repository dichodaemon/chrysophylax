# import dives.utility as ut
import os
import pandas as pd

# from dives.periodical import LatestSignals


class SignalChecker(object):
    def __init__(self, market_file, data_dir):
        self.market_df = pd.read_csv(market_file)
        self.market_df = self.market_df.set_index(["exchange", "pair"])
        self.market_df.sort_index(level=0, inplace=True)
        self.data_dir = data_dir
        self.dfs = {}
        self.paths = set()

    def fetch_tasks(self, price_row):
        market_rows = self.market_df.loc[[(price_row.exchange, price_row.pair)]]
        for m_row in market_rows.itertuples():
            date = "{}".format(price_row.time.date())
            task = ut.init_class("signal", m_row, date=date,
                                 pair=price_row.pair,
                                 exchange=price_row.exchange,
                                 period=m_row.period,
                                 destination_path=self.data_dir)
            yield task

    def match_thresholds(self, task, price_row):
        path = task.output().next().path
        if path not in self.paths:
            if not os.path.exists(path):
                return None
            self.dfs[path] = pd.read_csv(path, parse_dates=["time"])
            self.paths.add(path)
        r_df = self.dfs[path]
        previous_period = ut.previous_period(price_row.time, task.period)
        r_df = r_df[r_df.time == ut.previous_period(price_row.time,
                                                     task.period)]
        if len(r_df) > 0:
            return r_df.iloc[0]
        return None

    def new_signal_row(self, threshold_row, price_row):
        signal_row = dict(time=price_row.time,
                          pair=price_row.pair,
                          exchange=price_row.exchange,
                          period=threshold_row.period,
                          price=price_row.price)
        for prefix in ["long_entry", "long_exit",
                       "short_entry", "short_exit"]:
            s_value = getattr(threshold_row, "{}_value".format(prefix))
            s_type = getattr(threshold_row, "{}_type".format(prefix))
            signal = False
            if s_type == "price_gt":
                signal = price_row.price > s_value
            elif s_type == "price_lt":
                signal = price_row.price < s_value
            signal_row["{}_signal".format(prefix)] = signal
        return pd.DataFrame([signal_row]).iloc[0]

    def check(self, price_row):
        for task in self.fetch_tasks(price_row):
            threshold_row = self.match_thresholds(task, price_row)
            if threshold_row is not None:
                yield self.new_signal_row(threshold_row, price_row)

