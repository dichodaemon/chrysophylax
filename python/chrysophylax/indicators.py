import garm.indicators as gari
import ham.paths as hamp
import ham.time_utils as hamt
import ohlcv
import luigi
import os

from luigi.util import inherits


class Indicator(luigi.Task):
    pair = luigi.Parameter()
    exchange = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()
    FN = None
    COLUMN_NAME = ""

    def column_name(self):
        return self.COLUMN_NAME

    def output(self):
        parms = self.to_str_params()
        cls = self.__class__.__name__
        parms["class"] = cls
        path = hamp.path(hamp.DEFINITIONS[cls], **parms)
        path = os.path.join(self.destination_path, path)
        self.target = luigi.LocalTarget(path)
        yield self.target

    def run(self):
        self.target.makedirs()
        data = hamt.input_df(self.requires())
        name = self.column_name()
        data[name] = self.FN(data)
        next_m = hamt.next_month(self.month, False)
        data = data[self.month:next_m]
        data[[name]].to_csv(self.target.path, date_format=hamt.DATE_FORMAT)

@inherits(Indicator)
class WindowedIndicator(Indicator):
    window_size = luigi.IntParameter()
    DEFINITION = hamp.WINDOWED_INDICATOR

    def column_name(self):
        return self.COLUMN_NAME + "_{}".format(self.window_size)

    def requires(self):
        for t in self.requires_ohlcv():
            yield t

    def requires_ohlcv(self):
        for m in hamt.required_months(self.month, self.window_size, self.period):
            yield ohlcv.OHLCV(self.pair, self.exchange, m, self.period,
                                  self.destination_path)


@inherits(WindowedIndicator)
class ColumnStat(WindowedIndicator):
    input_column = luigi.Parameter()

    def column_name(self):
        return "{}_{}_{}".format(self.input_column,
                                 self.COLUMN_NAME,
                                 self.window_size)

@inherits(ColumnStat)
class MaxInWindow(ColumnStat):
    COLUMN_NAME = "max"
    FN = gari.max_in_window

@inherits(ColumnStat)
class MinInWindow(ColumnStat):
    COLUMN_NAME = "min"
    FN = gari.min_in_window

@inherits(Indicator)
class TrueRange(Indicator):
    COLUMN_NAME = "true_range"
    FN = gari.true_range

    def requires(self):
        yield ohlcv.OHLCV(self.pair, self.exchange,
                                   hamt.previous_month(self.month), self.period,
                                   self.destination_path)
        yield ohlcv.OHLCV(self.pair, self.exchange,
                                   self.month, self.period,
                                   self.destination_path)

@inherits(WindowedIndicator)
class AverageTrueRange(WindowedIndicator):
    COLUMN_NAME = "atr"
    FN = gari.average_true_range

    def requires(self):
        for m in hamt.required_months(self.month, self.window_size, self.period):
            yield TrueRange(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)

@inherits(WindowedIndicator)
class EfficiencyRatio(WindowedIndicator):
    COlUMN_NAME= "efficiency_ratio"
    FN = gari.efficiency_ratio


@inherits(WindowedIndicator)
class MovingAverage(ColumnStat):
    COLUMN_NAME = "ma"
    FN = gari.moving_average

