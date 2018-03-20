import garm.indicators as gari
import indicators as chi
import ohlcv
import luigi
import strategies as chs

from luigi.util import inherits


@inherit(chs.SignalThresholds)
class TurtleSoupSignalThresholds(ds.SignalThresholds):
    entry_window = luigi.IntParameter(default=20)
    wait = luigi.IntParameter(default=4)
    atr_window = luig.IntParameter(default=20)
    stop_loss_multiplier = luigi.FloatParameter(default=3.0)
    trailing_stop_multiplier = luigi.FloatParameter(default=3.0)
    FN = gari.turtle_soup_signals

    def requires(self):
        yield ohlcv.OHLCV(
                self.pair, self.exchange, self.date, self.period,
                self.destination_path)
        yield di.MinInWindow(self.pair, self.exchange, self.date,
                             self.period,
                             self.destination_path, entry_window, "low")
        yield di.AverageTrueRange(self.pair, self.exchange, self.date,
                                  self.period,
                                  self.destination_path, self.atr_window)
