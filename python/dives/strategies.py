import chrysophylax.indicators as chi
import chrysophylax.trade_manager as cht
import indicators as di
import downloads
import luigi
import os
import pandas as pd
import utility as ut

from luigi.contrib.simulate import RunAnywayTarget
from luigi.util import inherits


class Strategy(luigi.Task):
    pair = luigi.Parameter()
    period = luigi.Parameter(default="1d")
    exchange = luigi.Parameter()
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()
    balance = luigi.FloatParameter(default=100000.0)
    pyramiding = luigi.IntParameter(default=1)
    max_trade_percentage = luigi.FloatParameter(default=1.0)
    disable_longs = luigi.BoolParameter(default=False)
    disable_shorts = luigi.BoolParameter(default=False)
    destination_path = luigi.Parameter()

    def output(self):
        path = os.path.join(self.destination_path, "strategies",
                            ut.task_filename(self, "csv"))
        self.target = luigi.LocalTarget(path)
        yield self.target
        self.rerun = RunAnywayTarget(self)
        yield self.rerun


@inherits(Strategy)
class StrategyFlags(Strategy):
    FN = None

    def run(self):
        self.target.makedirs()
        data = ut.input_df(self.requires())
        self.FN(data)
        data.to_csv(self.target.path)
        if self.rerun is not None:
            self.rerun.done()


@inherits(Strategy)
class StrategyRun(Strategy):
    def run(self):
        data = pd.read_csv(self.requires().target.path,
                           index_col=0, parse_dates=True)
        trades = cht.execute_strategy(self, data)
        trades.to_csv(self.target.path)
        if self.rerun is not None:
            self.rerun.done()

@inherits(StrategyFlags)
class SimpleTurtleFlags(StrategyFlags):
    entry = luigi.IntParameter(default=20)
    exit = luigi.IntParameter(default=10)
    FN = chi.turtle_prepare_signals

    def requires(self):
        for m in ut.months(self.start_date, self.end_date):
            yield downloads.OHLCV(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)
            for days in set([self.entry, self.exit]):
                yield di.MaxInWindow(self.pair, self.exchange, m, self.period,
                                     self.destination_path, days, "high")
                yield di.MinInWindow(self.pair, self.exchange, m, self.period,
                                     self.destination_path, days, "low")
            yield di.AverageTrueRange(self.pair, self.exchange, m, self.period,
                                      self.destination_path, 20)

@inherits(SimpleTurtleFlags)
class SimpleTurtle(StrategyRun):
    stop_loss_multiplier = luigi.FloatParameter(default=0.0)
    trailing_stop_multiplier = luigi.FloatParameter(default=0.0)

    def requires(self):
        return SimpleTurtleFlags.from_str_params(self.to_str_params())


@inherits(StrategyFlags)
class BuyAndHoldFlags(StrategyFlags):
    FN = chi.buy_and_hold_prepare_signals

    def requires(self):
        for m in ut.months(self.start_date, self.end_date):
            yield downloads.OHLCV(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)

@inherits(BuyAndHoldFlags)
class BuyAndHold(StrategyRun):

    def requires(self):
        return BuyAndHoldFlags.from_str_params(self.to_str_params())
