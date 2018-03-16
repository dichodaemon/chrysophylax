import garm.trade_manager as gart
import luigi
import os
import pandas as pd
import utility as ut

from luigi.util import inherits


class SignalThresholds(luigi.Task):
    pair = luigi.Parameter()
    period = luigi.Parameter(default="1d")
    exchange = luigi.Parameter()
    date = luigi.DateParameter()
    destination_path = luigi.Parameter()

    FN = None
    COLS = ["long_entry_value", "long_entry_type",
            "long_exit_value", "long_exit_type",
            "short_entry_value", "short_entry_type",
            "short_exit_value", "short_exit_type"]

    def output(self):
        if ut.ongoing_month(self.date):
            suffix = "TMP-{:%m-%d_%H}"
            suffix = suffix.format(ut.latest_full_period(self.period))
            path = os.path.join(self.destination_path, "strategies",
                                ut.task_filename(self, "csv", suffix=suffix))
            self.target = luigi.LocalTarget(path)
            yield self.target
        else:
            path = os.path.join(self.destination_path, "strategies",
                                ut.task_filename(self, "csv"))
            self.target = luigi.LocalTarget(path)
            yield self.target

    def run(self):
        self.target.makedirs()
        data = ut.input_df(self.requires())
        data["period"] = self.period
        self.FN(data)
        col_set = set(self.COLS)
        cols = self.COLS[:]
        cols.extend([c for c in data.columns if c not in col_set])
        data[cols].to_csv(self.target.path, date_format=ut.DATE_FORMAT)

@inherits(SignalThresholds)
class Strategy(SignalThresholds):
    balance = luigi.FloatParameter(default=100000.0)
    pyramiding = luigi.IntParameter(default=1)
    max_trade_percentage = luigi.FloatParameter(default=1.0)
    disable_longs = luigi.BoolParameter(default=False)
    disable_shorts = luigi.BoolParameter(default=False)


@inherits(Strategy)
class StrategyRun(Strategy):
    COLS = ["direction", "contracts",
            "entry_time", "entry_label", "entry_price",
            "exit_time", "exit_label", "exit_price",
            "stop_loss", "trailing_stop_multiplier",
            "profit", "r_multiple",
            "initial_margin", "stop_loss_margin",
            "max_drawdown",
            "max_price", "min_price",
            "entry_value", "exit_value",
            "short_leverage"]
    def run(self):
        data = pd.read_csv(self.requires().target.path,
                           index_col=0, parse_dates=True)
        trades = gart.execute_strategy(self, data)
        if self.COLS is not None:
            trades = trades[self.COLS]
        trades.to_csv(self.target.path, date_format=ut.DATE_FORMAT)
