import pandas as pd

from data_sources import DataSource


class CSVSource(DataSource):
    def __init__(self, ohlcv_df, start_date, end_date, data_dir):
        self.ohlcv_df = ohlcv_df
        self.start_date = start_date
        self.end_date = end_date
        self.data_dir = data_dir
        self.current_index = 0

    def compute_price_df(self):
        prices = []
        for row in self.ohlcv_df.itertuples():
            columns = ["open", "low", "high", "close"]
            if row.open > row.close:
                columns = ["open", "high", "low", "close"]
            for c in columns:
                price = dict(time=row.time, exchange=row.exchange,
                              pair=row.pair,
                              price=getattr(row, c))
                prices.append(price)
        self.price_df = pd.DataFrame(prices)

    def __iter__(self):
        self.compute_price_df()
        return self

    def __fetch_data__(self):
        if self.current_index < len(self.price_df):
            row = self.price_df.iloc[self.current_index]
            self.current_index += 1
            return row
        else:
            raise StopIteration

