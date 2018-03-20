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
            values = []
            if row.open <= row.close:
                values.append(row.open)
                if row.low < row.open:
                    values.append(row.low)
                    values.append(0.5 * (row.low + row.open))
                values.append(row.open)
                values.append(0.5 * (row.open + row.close))
                values.append(row.close)
                if row.high > row.close:
                    values.append(row.high)
                    values.append(0.5 * (row.close + row.high))
                values.append(row.close)
            else:
                values.append(row.open)
                if row.high > row.open:
                    values.append(row.high)
                    values.append(0.5 * (row.high + row.open))
                values.append(row.open)
                values.append(0.5 * (row.open + row.close))
                values.append(row.close)
                if row.low > row.close:
                    values.append(row.low)
                    values.append(0.5 * (row.close + row.low))
                values.append(row.close)
            for v in values:
                price = dict(time=row.time, exchange=row.exchange,
                              pair=row.pair,
                              price=v)
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

