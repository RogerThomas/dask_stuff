#!/usr/bin/env python
import json
import s3fs
import random
import sys

import boto3
import numpy as np
import pandas as pd

from common import Config, set_aws_creds

sales = (np.arange(0, 1000) / 100).astype(np.float32).round(2)
print(sales.tolist())


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
        grossSales=np.random.choice(sales, n),
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


class S3Cli:
    def __init__(self, bucket='switching-large'):
        set_aws_creds()
        self._cli = boto3.client('s3')
        self._bucket = bucket
        self._cli.put_object(Bucket=bucket, Body='', Key='trans_product/')
        self._s3fs = s3fs.S3FileSystem(anon=False)

    def store_df(self, df, key, folder='trans_product'):
        path = '%s/%s/%s.parquet' % (self._bucket, folder, key)
        with self._s3fs.open(path, 'wb') as fh:
            df.to_parquet(fh, engine='pyarrow', compression='snappy')

    def store_obj_as_json(self, obj, key, folder='trans_product'):
        path = '%s/%s/%s.json' % (self._bucket, folder, key)
        with self._s3fs.open(path, 'wb') as fh:
            json_str = json.dumps(obj, indent=4, sort_keys=True)
            fh.write(json_str.encode('utf8'))


def make_data():
    s3_cli = S3Cli()
    metadata = {'num_rows': 0, 'memory_mb': 0, 'memory_gb': 0}
    current_transaction_key = 0
    for date in Config.dates:
        print(date)
        baskets, df = make_df(current_transaction_key)
        current_transaction_key += len(baskets)
        date_str = date_to_fname(date)
        df = group_and_sum_transactions(df)
        df['transactionDate'] = date
        # fname = 'data/%s.parquet' % (date_str,)
        # df.to_parquet(fname, engine='fastparquet')
        key = date_str
        s3_cli.store_df(df, key)
        memory_mb = df.memory_usage(index=True).sum() / (1000 * 1000)
        print('DF Size: %s Mem MB: %s' % (len(df), memory_mb))
        metadata['num_rows'] += len(df)
        metadata['memory_mb'] += memory_mb
        metadata['memory_gb'] += memory_mb / 1000
        s3_cli.store_obj_as_json(metadata, 'metadata')
        print(metadata)


def date_to_fname(date):
    return str(date.date()).replace('-', '_')


def main(*args):
    make_data()


if __name__ == '__main__':
    main(*sys.argv[1:])
