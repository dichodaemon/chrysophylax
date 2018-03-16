import luigi
import os
import pandas as pd
import pytz
import strategies as ds
import utility as ut

from gilles.signal_checker import SignalChecker
from csv_source import CSVSource
from luigi.util import inherits


class Study(luigi.Task):
    markets = luigi.Parameter()
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()
    destination_path = luigi.Parameter()


    def output(self):
        suffix = "OPEN"
        path = os.path.join(self.destination_path, "studies",
                            ut.task_filename(self, "csv", suffix=suffix,
                                             exclude=["markets"]))
        self.open = luigi.LocalTarget(path)
        yield self.open
        suffix = "CLOSED"
        path = os.path.join(self.destination_path, "studies",
                            ut.task_filename(self, "csv", suffix=suffix,
                                             exclude=["markets"]))
        self.closed = luigi.LocalTarget(path)
        yield self.closed

    def run(self):
        self.open.makedirs()
        price_source = CSVSource(self.markets, self.start_date, self.end_date,
                                 self.destination_path)
        signal_checker = SignalChecker(self.markets, self.destination_path)
        for t in price_source.tasks:
            yield t
        for price_row in price_source:
            for t in signal_checker.fetch_tasks():
                yield t
                threshold_row = signal_checker.match_thresholds(task, price_row)
                if threshold_row is not None:
                    print self.new_signal_row(threshold_row, price_row)



