import garm.indicators as gari
import indicators as di
import ohlcv
import luigi
import strategies as chs

from luigi.util import inherits


@inherits(chs.Strategy)
class SimpleTurtle(chs.Strategy):
    entry = luigi.IntParameter(default=20)
    exit = luigi.IntParameter(default=10)
    atr_window = luigi.IntParameter(default=20)
    stop_loss_multiplier = luigi.FloatParameter(default=3.0)
    trailing_stop_multiplier = luigi.FloatParameter(default=3.0)
    FN = gari.simple_turtle_signals

    def requires(self):
        yield ohlcv.OHLCV(
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
                                  self.destination_path, self.atr_window)
