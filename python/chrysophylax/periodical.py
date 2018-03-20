import ham.time_utils as hamt
import luigi
import os
import pandas as pd
import pytz
import strategies as ds

from luigi.util import inherits


class LatestSignals(luigi.Task):
    markets = luigi.Parameter()
    period = luigi.Parameter(default="1h")
    destination_path = luigi.Parameter()

    def requires(self):
        self.signals = {}
        markets = pd.read_csv(self.markets)
        for row in markets.itertuples():
            if row.exchange not in self.signals:
                self.signals[row.exchange] = {}
            cur_exchange = self.signals[row.exchange]
            if row.pair not in cur_exchange:
                cur_exchange[row.pair] = []
            cur_pair = cur_exchange[row.pair]
            date = "{}".format(pytz.datetime.datetime.utcnow().date())
            task = hamt.init_class("signal", row, date=date, pair=row.pair,
                                 exchange=row.exchange, period=row.period,
                                 destination_path=self.destination_path)
            cur_pair.append(task)
            yield task

    def output(self):
        suffix = "TMP-{:%Y-%m-%d_%H}"
        suffix = suffix.format(hamt.latest_full_period(self.period))
        path = os.path.join(self.destination_path, "operation",
                            hamt.task_filename(self, "csv", suffix=suffix,
                                             exclude=["markets"]))
        self.target = luigi.LocalTarget(path)
        yield self.target

    def run(self):
        self.target.makedirs()
        result = []
        for exchange, pairs in self.signals.items():
            for pair, strategies in pairs.items():
                for strategy in strategies:
                    signal_df = hamt.input_df([strategy])
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
                                   short_exit_type=row.short_exit_type,
                                   stop_loss_delat=row.stop_loss_delta,
                                   trailing_stop_delta=row.trailing_stop_delta)
                    result.append(new_row)
        result.sort(key=lambda v: v["pair"])
        cols = ["time", "period", "exchange", "pair", "strategy"]
        cols.extend(ds.SignalThresholds.COLS)
        print self.target.path
        result_df = pd.DataFrame(result)
        result_df.to_csv(self.target.path, date_format=hamt.DATE_FORMAT)
