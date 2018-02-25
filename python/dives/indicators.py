import chrysophylax.indicators as chi
import downloads
import luigi
import os
import utility as ut

from luigi.contrib.simulate import RunAnywayTarget
from luigi.util import inherits


class Indicator(luigi.Task):
    pair = luigi.Parameter()
    exchange = luigi.Parameter()
    month = luigi.MonthParameter()
    period = luigi.Parameter(default="1d")
    destination_path = luigi.Parameter()
    COLUMN_NAME = ""
    FILE_NAME = ""
    FN = None

    def column_name(self):
        return self.COLUMN_NAME

    def output(self):
        if ut.ongoing_month(self.month):
            path = os.path.join(self.destination_path,
                                ut.task_filename(self, "csv", suffix="PARTIAL"))
            self.target = luigi.LocalTarget(path)
            yield self.target
            self.rerun = RunAnywayTarget(self)
            yield self.rerun
        else:
            path = os.path.join(self.destination_path,
                                ut.task_filename(self, "csv"))
            self.target = luigi.LocalTarget(path)
            yield self.target
            self.rerun = None

    def run(self):
        data = ut.input_df(self)
        name = self.column_name()
        data[name] = self.FN(data)
        next_m = ut.next_month(self.month, False)
        data = data[self.month:next_m]
        data[[name]].to_csv(self.target.path)
        if not self.rerun is None:
            self.rerun.done()

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
            yield downloads.OHLCV(self.pair, self.exchange, m, self.period,
                                  self.destination_path)


@inherits(WindowedIndicator)
class MaxInWindow(WindowedIndicator):
    COLUMN_NAME = "max"
    FN = chi.max_in_window

@inherits(WindowedIndicator)
class MinInWindow(WindowedIndicator):
    COLUMN_NAME = "min"
    FN = chi.min_in_window

@inherits(Indicator)
class TrueRange(Indicator):
    COLUMN_NAME = "true_range"
    FN = chi.true_range

    def requires(self):
        yield downloads.OHLCV(self.pair, self.exchange,
                                   ut.previous_month(self.month), self.period,
                                   self.destination_path)
        yield downloads.OHLCV(self.pair, self.exchange,
                                   self.month, self.period,
                                   self.destination_path)

@inherits(WindowedIndicator)
class AverageTrueRange(WindowedIndicator):
    COLUMN_NAME = "atr"
    FN = chi.average_true_range

    def requires(self):
        for m in ut.required_months(self.month, self.window_size, self.period):
            yield TrueRange(
                    self.pair, self.exchange, m, self.period,
                    self.destination_path)

@inherits(WindowedIndicator)
class VolumeMovingAverage(WindowedIndicator):
    COLUMN_NAME = "volume_ma"
    FN = chi.volume_ma

