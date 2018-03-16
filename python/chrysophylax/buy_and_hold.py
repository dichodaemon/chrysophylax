import garm.indicators as gari
import ohlcv
import luigi
import strategies as ds
import ham.time_utils as hamt

from luigi.util import inherits


@inherits(ds.SignalThresholds)
class BuyAndHoldFlags(ds.SignalThresholds):
    FN = gari.buy_and_hold_prepare_signals

    def requires(self):
        for m in hamt.months(self.start_date, self.end_date):
            yield ohlcv.OHLCV(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)

@inherits(BuyAndHoldFlags)
class BuyAndHold(ds.StrategyRun):

    def requires(self):
        return BuyAndHoldFlags.from_str_params(self.to_str_params())
