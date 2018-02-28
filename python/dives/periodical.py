import charts
import chrysophylax.indicators as chi
import datetime
import downloads
import indicators as di
import luigi
import os
import pandas as pd
import strategies
import utility as ut

from luigi.contrib.simulate import RunAnywayTarget
from luigi.util import inherits


class Periodical(luigi.Task):
    markets = luigi.Parameter()
    period = luigi.Parameter(default="1d")
    start_date = luigi.DateParameter()
    balance = luigi.FloatParameter(default=100000.0)
    pyramiding = luigi.IntParameter(default=1)
    max_trade_percentage = luigi.FloatParameter(default=1.0)
    disable_longs = luigi.BoolParameter(default=False)
    disable_shorts = luigi.BoolParameter(default=False)
    stop_loss_multiplier = luigi.FloatParameter(default=0.0)
    trailing_stop_multiplier = luigi.FloatParameter(default=0.0)
    destination_path = luigi.Parameter()

    def requires(self):
        markets = pd.read_csv(self.markets)
        for row in markets.itertuples():
            yield charts.TurtlePlot(pair=row.pair, period=self.period,
                                    exchange=row.exchange,
                                    start_date=self.start_date,
                                    end_date=datetime.datetime.now(),
                                    balance=self.balance,
                                    pyramiding=self.pyramiding,
                                    max_trade_percentage=self.max_trade_percentage,
                                    stop_loss_multiplier=self.stop_loss_multiplier,
                                    trailing_stop_multiplier=self.trailing_stop_multiplier,
                                    disable_longs=self.disable_longs,
                                    disable_shorts=self.disable_shorts,
                                    destination_path=self.destination_path)
            break


    def output(self):
        self.rerun = RunAnywayTarget(self)
        yield self.rerun

    def run(self):
        self.rerun.done()

