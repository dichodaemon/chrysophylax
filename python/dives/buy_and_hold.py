import chrysophylax.indicators as chi
import downloads
import luigi
import strategies as ds
import utility as ut

from luigi.util import inherits


@inherits(ds.SignalThresholds)
class BuyAndHoldFlags(ds.SignalThresholds):
    FN = chi.buy_and_hold_prepare_signals

    def requires(self):
        for m in ut.months(self.start_date, self.end_date):
            yield downloads.OHLCV(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)

@inherits(BuyAndHoldFlags)
class BuyAndHold(ds.StrategyRun):

    def requires(self):
        return BuyAndHoldFlags.from_str_params(self.to_str_params())
