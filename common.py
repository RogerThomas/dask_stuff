import pandas as pd


class Config:
    num_products = 600
    num_stores = 1000
    num_customers = 1000
    start_date = '01/01/2017'
    end_data = '03/30/2017'
    num_weeks = 5
    min_transactions = 15_000_000
    max_transactions = 20_000_000
    min_transactions = 150_000
    max_transactions = 200_000
    dates = pd.date_range(start=start_date, periods=num_weeks, freq='1W-MON').tolist()
    dates = pd.date_range(start=start_date, periods=365).tolist()
    min_basket_size = 1
    max_basket_size = 50


class Logger:
    def info(self, msg):
        print(msg)


logger = Logger()