import chrysophylax.indicators as chi
import indicators as di
import downloads
import luigi
import strategies as ds
import utility as ut

from luigi.util import inherits


@inherits(ds.SignalThresholds)
class SimpleTurtleSignalThresholds(ds.SignalThresholds):
    entry = luigi.IntParameter(default=20)
    exit = luigi.IntParameter(default=10)
    FN = chi.turtle_prepare_signals

    def requires(self):
        yield downloads.OHLCV(
                self.pair, self.exchange, self.date, self.period,
                self.destination_path)
        for days in set([self.entry, self.exit]):
            yield di.MaxInWindow(self.pair, self.exchange, self.date,
                                 self.period,
                                 self.destination_path, days, "high")
            yield di.MinInWindow(self.pair, self.exchange, self.date,
                                 self.period,
                                 self.destination_path, days, "low")
        yield di.AverageTrueRange(self.pair, self.exchange, self.date,
                                  self.period,
                                  self.destination_path, 20)

@inherits(SimpleTurtleSignalThresholds)
class SimpleTurtle(ds.StrategyRun):
    stop_loss_multiplier = luigi.FloatParameter(default=0.0)
    trailing_stop_multiplier = luigi.FloatParameter(default=0.0)
    balance_pctg = luigi.FloatParameter(default=1.0)
    short_leverage = luigi.FloatParameter(default=2.0)

    def requires(self):
        return SimpleTurtleFlags.from_str_params(self.to_str_params())

