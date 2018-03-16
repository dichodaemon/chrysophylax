import garm.indicators as gari
import ohlcv
import luigi
import os
import utility as ut

from luigi.util import inherits


class Indicator(luigi.Task):
    pair = luigi.Parameter()
    exchange = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()
    COLUMN_NAME = ""
    FN = None

    def column_name(self):
        return self.COLUMN_NAME

    def output(self):
        if ut.ongoing_month(self.month):
            suffix = "TMP-{:%m-%d_%H}"
            suffix = suffix.format(ut.latest_full_period(self.period))
            path = os.path.join(self.destination_path, "indicators",
                                ut.task_filename(self, "csv", suffix=suffix))
            self.target = luigi.LocalTarget(path)
            yield self.target
        else:
            path = os.path.join(self.destination_path, "indicators",
                                ut.task_filename(self, "csv"))
            self.target = luigi.LocalTarget(path)
            yield self.target

    def run(self):
        self.target.makedirs()
        data = ut.input_df(self.requires())
        name = self.column_name()
        data[name] = self.FN(data)
        next_m = ut.next_month(self.month, False)
        data = data[self.month:next_m]
        data[[name]].to_csv(self.target.path, date_format=ut.DATE_FORMAT)

@inherits(Indicator)
class WindowedIndicator(Indicator):
    window_size = luigi.IntParameter()

    def column_name(self):
        return self.COLUMN_NAME + "_{}".format(self.window_size)

    def requires(self):
        for t in self.requires_ohlcv():
            yield t

    def requires_ohlcv(self):
        for m in ut.required_months(self.month, self.window_size, self.period):
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
                                   ut.previous_month(self.month), self.period,
                                   self.destination_path)
        yield ohlcv.OHLCV(self.pair, self.exchange,
                                   self.month, self.period,
                                   self.destination_path)

@inherits(WindowedIndicator)
class AverageTrueRange(WindowedIndicator):
    COLUMN_NAME = "atr"
    FN = gari.average_true_range

    def requires(self):
        for m in ut.required_months(self.month, self.window_size, self.period):
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

