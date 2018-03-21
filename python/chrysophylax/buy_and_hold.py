import garm.indicators as gari
import ham.time_utils as hamt
import ohlcv
import luigi
import strategies as chs

from luigi.util import inherits


@inherits(chs.Strategy)
class BuyAndHold(chs.Strategy):
    FN = gari.buy_and_hold_signals

    def requires(self):
        for m in hamt.months(self.start_date, self.end_date):
            yield ohlcv.OHLCV(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)
