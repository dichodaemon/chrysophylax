import ham.paths as hamp
import ham.time_utils as hamt
import ham.time_utils as hamt
import luigi
import os
import pandas as pd

from luigi.util import inherits


class Strategy(luigi.Task):
    pair = luigi.Parameter()
    period = luigi.Parameter(default="1d")
    exchange = luigi.Parameter()
    date = luigi.DateParameter()
    destination_path = luigi.Parameter()

    FN = None
    COLS = ["long_entry_value", "long_entry_type",
            "long_exit_value", "long_exit_type",
            "long_setup_value", "long_setup_type",
            "short_entry_value", "short_entry_type",
            "short_exit_value", "short_exit_type",
            "short_setup_value", "short_setup_type",
            "stop_loss_delta", "trailing_stop_delta"]

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
        data["period"] = self.period
        self.FN(data)
        col_set = set(self.COLS)
        cols = self.COLS[:]
        cols.extend([c for c in data.columns if c not in col_set])
        data[cols].to_csv(self.target.path, date_format=hamt.DATE_FORMAT)
