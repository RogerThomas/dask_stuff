#!/usr/bin/env python
import pandas as pd
import numpy as np
import random
import json
import sys
from common import Config


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


def make_df(current_transaction_key):
    n = random.randint(Config.min_transactions, Config.max_transactions)
    baskets = generate_baskets(n)
    transaction_keys = np.arange(current_transaction_key, current_transaction_key + len(baskets))

    def repeat_arr(min_val, max_val):
        arr = np.random.randint(min_val, max_val, len(baskets), dtype=np.int32)
        arr = np.repeat(arr, baskets)
        return arr

    df = pd.DataFrame(dict(
        transactionKey=np.repeat(transaction_keys, baskets),
        productKey=np.random.randint(1, Config.num_products + 1, n, dtype=np.int32),
        storeKey=repeat_arr(1, Config.num_stores + 1),
        customerKey=repeat_arr(1, Config.num_customers + 1),
        grossSales=np.random.random(n).astype(np.float32),
    ))
    df['netSales'] = df['grossSales']
    return baskets, df


def group_and_sum_transactions(df):
    df['volume'] = 1
    df['unitVolume'] = 1
    sum_fields = ['grossSales', 'netSales', 'unitVolume', 'volume']
    gb_fields = ['customerKey', 'productKey', 'transactionKey']
    df = df.groupby(gb_fields, as_index=False)[sum_fields].sum()
    return df


def make_data():
    metadata = {'num_rows': 0, 'memory_mb': 0, 'memory_gb': 0}
    current_transaction_key = 0
    for date in Config.dates:
        print(date)
        baskets, df = make_df(current_transaction_key)
        current_transaction_key += len(baskets)
        date_str = date_to_fname(date)
        df = group_and_sum_transactions(df)
        df['transactionDate'] = date
        fname = 'data/%s.parquet' % (date_str,)
        df.to_parquet(fname, engine='fastparquet')
        memory_mb = df.memory_usage(index=True).sum() / (1000 * 1000)
        print('DF Size: %s Mem MB: %s' % (len(df), memory_mb))
        metadata['num_rows'] += len(df)
        metadata['memory_mb'] += memory_mb
        metadata['memory_gb'] += memory_mb / 1000
        print(metadata)
        with open('data/metadata.json', 'w+') as fh:
            json.dump(metadata, fh, sort_keys=True, indent=4)


def get_cann_group_ser(product_keys):
    cann_groups = pd.Series(data=product_keys % 10, index=product_keys)
    return cann_groups


def get_dates():
    week_start_dates = pd.date_range(start='2017/01/01', periods=52, freq='W-MON')
    dates = []
    for week_start_date in week_start_dates:
        day_dates = pd.date_range(start=week_start_date, periods=7)
        dates.append((week_start_date, day_dates))
    return dates


def date_to_fname(date):
    return str(date.date()).replace('-', '_')


def calculate():
    """
    print(df)
    return
    products = np.array(range(10, 109))
    df = dd.read_parquet('data/2017_01_0*.parquet')
    df = df.groupby(['productKey', 'storeKey']).sum()
    df = df.compute()
    thing = 1

    def f(df):
        if thing:
            return df
        df = df[df['productKey'].isin(products)].copy()
        df['volume'] = 1
        df['unitVolume'] = 1
        sum_fields = ['volume', 'unitVolume', 'grossSales', 'netSales']
        print(len(df))
        df = df.groupby(['storeKey', 'productKey'])[sum_fields].sum()
        print(len(df))
        df = df.reset_index()
        return df
    dates = get_dates()
    for week_start_date, days in dates:
        print(week_start_date)
        fnames = ['data/%s.parquet' % date_to_fname(day) for day in days]
        df = dd.read_parquet(fnames)
        df = df.map_partitions(f)
        if thing:
            df = df[df['productKey'].isin(products)]
            df['volume'] = 1
            df['unitVolume'] = 1
        print(len(df))
        sum_fields = ['volume', 'unitVolume', 'grossSales', 'netSales']
        df = df.groupby(['storeKey', 'productKey'])[sum_fields].sum()
        df = df.compute()
        print(df['volume'].sum())
        df = df.reset_index()
        df['weekStartDate'] = week_start_date
        fname = 'week_store_product/%s.parquet' % date_to_fname(week_start_date)
        df.to_parquet(fname, engine='fastparquet')
        return
    """


def main(*args):
    make_data()
    return
    if args[0] == 'make':
        print('making')
    else:
        calculate()
    return
    """
    make_data()
    df = pd.read_parquet('data/2017_01_10.parquet')
    df = pd.read_parquet('data/2017_01_01.parquet')
    df = df[df['productKey'].isin(product_keys)]
    df['td'] = df.index
    df['transprodseqnum'] = df.groupby(
        ['customerKey', 'productKey']
    )['td', 'transactionDate'].rank(method='dense')
    print(df)
    return
    df = pd.read_parquet('data/2017_01_01.parquet')
    print(df)
    return
    df = df.sort_values('transactionDate')

    product_keys = np.arange(4, 100)
    print(client)
    print(type(df))
    print(df.npartitions)
    print(df.head())
    cann_groups = get_cann_group_ser(product_keys)
    df['cannGroup'] = df['productKey'].map(cann_groups)
    df = df.sort_values('transactionDate')
    df = client.perists(df)
    df = df.groupby('cannGroup')[['netSales']].sum()
    df = df.reset_index()
    df = df.compute()
    print(df)
    """


if __name__ == '__main__':
    main(*sys.argv[1:])
