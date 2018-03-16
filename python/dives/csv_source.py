import downloads
import pandas as pd
import utility as ut

from garm.data_sources import DataSource


class CSVSource(DataSource):
    def __init__(self, ticker_file, start_date, end_date, data_dir):
        self.ticker_df = pd.read_csv(ticker_file, index_col=0, parse_dates=True)
        self.start_date = start_date
        self.end_date = end_date
        self.data_dir = data_dir
        self.tasks = set()
        self.current_index = 0
        self.initialize_tasks()

    def initialize_tasks(self):
        for m in ut.months(self.start_date, self.end_date):
            for ticker_row in self.ticker_df.itertuples():
                task = downloads.OHLCV(pair=ticker_row.pair,
                                       exchange=ticker_row.exchange,
                                       month=m,
                                       period=ticker_row.period,
                                       destination_path=self.data_dir)
                self.tasks.add(task)

    def merge_df(self):
        ohlcv = []
        for t in self.tasks:
            df = pd.read_csv(t.target.path, parse_dates=True)
            df["exchange"] = t.exchange
            df["pair"] = t.pair
            df["period"] = t.period
            ohlcv.append(df)
        ohlcv_df = pd.concat(ohlcv)
        ohlcv_df.set_index("time", inplace=True)
        ohlcv_df.sort_index(level=0, inplace=True)
        ohlcv_df.reset_index()
        prices = []
        for row in ohlcv_df.itertuples():
            columns = ["open", "low", "high", "close"]
            if row.open > row.close:
                columns = ["open", "high", "low", "close"]
            for c in columns:
                price = dict(time=row.time, exchange=row.exchange,
                              pair=row.pair,
                              price=getattr(row, c))
                prices.append(price)
        return DataFrame(prices)

    def __iter__(self):
        self.df = self.merge_df()
        return self

    def __fetch_data__(self):
        if self.current_index < len(self.df):
            row = self.df.iloc[self.current_index]
            self.current_index += 1
            return row

