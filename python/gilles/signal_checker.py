import ham.paths as hamp
import ham.time_utils as hamt
import json
import os
import pandas as pd

class SignalChecker(object):
    def __init__(self, market_file, data_dir):
        self.market_df = pd.read_csv(market_file)
        self.market_df = self.market_df.set_index(["exchange", "pair"])
        self.market_df.sort_index(level=0, inplace=True)
        self.data_dir = data_dir
        self.thr_dfs = {}
        self.paths = set()

    def fetch_threshold_paths(self, price_row):
        market_rows = self.market_df.loc[[(price_row.exchange, price_row.pair)]]
        for m_row in market_rows.itertuples():
            date = "{}".format(price_row.time.date())
            task = m_row.signal_task
            args = dict(date=date, pair=price_row.pair,
                        exchange=price_row.exchange,
                        period=m_row.period,
                        destination_path=self.data_dir)
            args.update(json.loads(m_row.signal_parameters))
            args.update({"class": task})
            path = hamp.path(hamp.DEFINITIONS[task], **args)
            yield m_row, os.path.join(self.data_dir, path)

    def match_thresholds(self, m_row, path, price_row):
        if path not in self.paths:
            if not os.path.exists(path):
                print "{} not found".format(path)
                if not m_row in self.thr_dfs:
                    return None
            else:
                self.thr_dfs[m_row] = pd.read_csv(path, parse_dates=["time"])
                self.paths.add(path)
        r_df = self.thr_dfs[m_row]
        previous_period = hamt.previous_period(price_row.time, m_row.period)
        r_df = r_df[r_df.time == hamt.previous_period(price_row.time,
                                                     m_row.period)]
        if len(r_df) > 0:
            return r_df.iloc[0]
        return None

    def new_signal_row(self, threshold_row, price_row):
        signal_row = dict(time=price_row.time,
                          pair=price_row.pair,
                          exchange=price_row.exchange,
                          period=threshold_row.period,
                          price=price_row.price,
                          stop_loss_delta=threshold_row.stop_loss_delta,
                          trailing_stop_delta=threshold_row.trailing_stop_delta)
        for direction in ["long", "short"]:
            exp_locals = dict(trigger=threshold_row, ticker=price_row)
            setup = threshold_row["{}_setup".format(direction)]
            if isinstance(setup, str):
                setup = eval(setup, {}, exp_locals)
            entry = threshold_row["{}_entry".format(direction)]
            if isinstance(entry, str):
                entry = eval(entry, {}, exp_locals)
            exit = threshold_row["{}_exit".format(direction)]
            if isinstance(exit, str):
                exit = eval(exit, {}, exp_locals)
            signal_row["{}_entry_signal".format(direction)] = setup and entry
            signal_row["{}_exit_signal".format(direction)] = exit
        return pd.DataFrame([signal_row]).iloc[0]

    def check(self, price_row):
        for m_row, path in self.fetch_threshold_paths(price_row):
            threshold_row = self.match_thresholds(m_row, path, price_row)
            if threshold_row is not None:
                yield self.new_signal_row(threshold_row, price_row)
