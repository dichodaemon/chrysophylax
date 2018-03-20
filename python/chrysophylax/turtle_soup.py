import garm.indicators as gari
import indicators as chi
import ohlcv
import luigi
import strategies as chs

from luigi.util import inherits


@inherits(chs.SignalThresholds)
class TurtleSoupSignalThresholds(chs.SignalThresholds):
    entry_window = luigi.IntParameter(default=20)
    wait = luigi.IntParameter(default=4)
    atr_window = luigi.IntParameter(default=20)
    entry_multiplier = luigi.FloatParameter(default=2.5)
    exit_multiplier = luigi.FloatParameter(default=0.5)
    trailing_stop_multiplier = luigi.FloatParameter(default=1.0)
    FN = gari.turtle_soup_signals

    def requires(self):
        yield ohlcv.OHLCV(
                self.pair, self.exchange, self.date, self.period,
                self.destination_path)
        yield chi.MinInWindow(self.pair, self.exchange, self.date,
                              self.period,
                              self.destination_path, self.entry_window, "low")
        yield chi.AverageTrueRange(self.pair, self.exchange, self.date,
                                   self.period,
                                   self.destination_path, self.atr_window)
