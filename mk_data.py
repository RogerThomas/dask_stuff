#!/usr/bin/env python
import json
import random
import sys

import numpy as np
import pandas as pd

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


def date_to_fname(date):
    return str(date.date()).replace('-', '_')


def main(*args):
    make_data()


if __name__ == '__main__':
    main(*sys.argv[1:])
