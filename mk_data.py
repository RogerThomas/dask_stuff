#!/usr/bin/env python
import pandas as pd
import numpy as np
import random
import dask.dataframe as dd
from dask.distributed import Client


class Config:
    num_products = 1000
    num_stores = 1000
    num_customers = 1000
    start_date = '01/01/2017'
    end_data = '03/30/2017'
    min_transactions = 10_000_000
    max_transactions = 50_000_000
    min_basket_size = 1
    max_basket_size = 50


def generate_baskets(n):
    baskets = []
    current_n = 0
    while True:
        basket = random.randint(Config.min_basket_size, Config.max_basket_size)
        if current_n == n:
            break
        if (current_n + basket) > n:
            basket = n - current_n
        current_n += basket
        baskets.append(basket)
    if current_n < n:
        baskets.append(n - current_n)
    return np.array(baskets)


def repeat_arr(min_val, max_val, repeat_arr):
    arr = np.random.randint(min_val, max_val, len(repeat_arr), dtype=np.int32)
    arr = np.repeat(arr, repeat_arr)
    return arr


def make_data():
    n = random.randint(Config.min_transactions, Config.max_transactions)
    baskets = generate_baskets(n)
    df = pd.DataFrame(dict(
        productKey=np.random.randint(1, Config.num_products + 1, n, dtype=np.int32),
        storeKey=repeat_arr(1, Config.num_stores + 1, baskets),
        customerKey=repeat_arr(1, Config.num_customers + 1, baskets),
        grossSales=np.random.random(n).astype(np.float32),
    ))
    df['netSales'] = df['grossSales']
    return df


def get_cann_group_ser():
    arr = np.arange(1, Config.num_products + 1)
    cann_groups = pd.Series(data=arr % 10, index=arr)
    return cann_groups


def main():
    """
    df = pd.read_parquet('data/2017_01_10.parquet')
    print(df.memory_usage(index=True).sum() /(1000 * 1000))
    print(df.info(memory_usage='deep'))
    return
    """
    client = Client('127.0.0.1:8786')
    print(client)
    df = dd.read_parquet('data/*')
    print(df.npartitions)
    df = df[df['productKey'].isin(np.arange(4, 100))]
    print(df.head())
    cann_groups = get_cann_group_ser()
    df['cannGroup'] = df['productKey'].map(cann_groups)
    df = df.groupby(['storeKey', 'weekStartDate', 'cannGroup'])[['netSales']].sum()
    df = df.reset_index()
    df = df.compute()
    print(df)
    print(type(df))
    print(len(df))
    print(df)
    print(df)
    return
    dates = pd.date_range(Config.start_date, Config.end_data)
    for date in dates:
        df = make_data()
        date_str = str(date.date()).replace('-', '_')
        df['weekStartDate'] = date
        fname = 'data/%s.parquet' % (date_str,)
        df.to_parquet(fname, engine='fastparquet')
        print(date)
        print(df.memory_usage(index=True).sum() / (1000 * 1000))


if __name__ == '__main__':
    main()
