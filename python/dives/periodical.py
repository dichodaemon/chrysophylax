import charts
import chrysophylax.indicators as chi
import datetime
import downloads
import indicators as di
import luigi
import os
import pandas as pd
import pytz
import simple_turtle as st
import strategies as ds
import utility as ut

from luigi.util import inherits


class LatestSignals(luigi.Task):
    markets = luigi.Parameter()
    period = luigi.Parameter(default="1h")
    destination_path = luigi.Parameter()

    def requires(self):
        self.signals = {}
        markets = pd.read_csv(self.markets)
        # count = 0
        for row in markets.itertuples():
            # count += 1
            # if count > 10:
                # break
            if row.exchange not in self.signals:
                self.signals[row.exchange] = {}
            cur_exchange = self.signals[row.exchange]
            if row.pair not in cur_exchange:
                cur_exchange[row.pair] = []
            cur_pair = cur_exchange[row.pair]
            t_params = self.to_str_params()
            date = "{}".format(pytz.datetime.datetime.utcnow().date())
            t_params["date"] = date
            t_params["pair"] = row.pair
            t_params["exchange"] = row.exchange
            t_params["period"] = row.period
            task = st.SimpleTurtleSignalThresholds.from_str_params(t_params)
            cur_pair.append(task)
            yield task

    def output(self):
        suffix = "TMP-{:%Y-%m-%d_%H}"
        suffix = suffix.format(ut.latest_full_period(self.period))
        path = os.path.join(self.destination_path, "operation",
                            ut.task_filename(self, "csv", suffix=suffix,
                                             exclude=["markets"]))
        self.target = luigi.LocalTarget(path)
        yield self.target

    def run(self):
        self.target.makedirs()
        result = []
        for exchange, pairs in self.signals.items():
            for pair, strategies in pairs.items():
                for strategy in strategies:
                    signal_df = ut.input_df([strategy])
                    if len(signal_df) == 0:
                        continue
                    row = signal_df.iloc[-1]
                    new_row = dict(time=signal_df.index[-1],
                                   period = row.period,
                                   exchange=strategy.exchange,
                                   pair=strategy.pair,
                                   strategy=strategy.__class__.__name__,
                                   long_entry_value=row.long_entry_value,
                                   short_entry_value=row.short_entry_value,
                                   long_exit_value=row.long_exit_value,
                                   short_exit_value=row.short_exit_value,
                                   long_entry_type=row.long_entry_type,
                                   short_entry_type=row.short_entry_type,
                                   long_exit_type=row.long_exit_type,
                                   short_exit_type=row.short_exit_type)
                    result.append(new_row)
        result.sort(key=lambda v: v["pair"])
        cols = ["time", "period", "exchange", "pair", "strategy"]
        cols.extend(ds.SignalThresholds.COLS)
        result_df = pd.DataFrame(result)[cols]
        result_df.to_csv(self.target.path, date_format=ut.DATE_FORMAT)
