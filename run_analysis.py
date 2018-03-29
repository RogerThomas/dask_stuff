#!/usr/bin/env python
import random
import time

import dask.dataframe as ddf
import pandas as pd
import numpy as np
from dask.distributed import Client
from common import Config, logger


client = Client('127.0.0.1:8786')
ncores = sum(client.ncores().values())


class Timer:
    def __init__(self, msg):
        self._msg = msg

    def __enter__(self):
        self._t1 = time.time()
        return self

    def __exit__(self, error_type, error_value, tb):
        if error_type is not None:
            raise
        print('%s took: %s' % (self._msg, time.time() - self._t1))


def make_cann_group_df():
    random.seed(10)
    cgs = [(1, 'Nestle'), (2, 'Cadbury'), (3, 'Other')]
    cann_group_data = []
    ps = set()
    for _ in range(10):
        while True:
            product_key = random.randint(0, Config.num_products)
            if product_key not in ps:
                ps.add(product_key)
                break
        cann_group = random.choice(cgs)
        cann_group_data.append(dict(
            productKey=product_key,
            cannGroupKey=cann_group[0],
            cannGroupDesc=cann_group[1],
        ))
    cann_group_df = pd.DataFrame(cann_group_data)
    cann_group_df = cann_group_df.set_index('productKey', drop=False)
    return cann_group_df


def filter_and_map_cann_groups(df, cann_group_df):
    df = df[df['productKey'].isin(cann_group_df['productKey'])]
    df['cannGroupKey'] = df['productKey'].map(cann_group_df['cannGroupKey'])
    return df


def trans_seq_tot(df):
    df = df.copy()
    df['productCount'] = np.ones(len(df), dtype='u1')
    df = df.groupby(
        ['customerKey', 'transSeqNum'], as_index=False
    )['grossSales', 'netSales'].sum()
    return df


def calc_spend_switch(seq_num_df, within_trans=True):
    merge_fields = ['customerKey', 'transSeqNum']
    seq_tot_df = trans_seq_tot(seq_num_df)
    seq_tot_df_a = seq_tot_df
    seq_tot_df_b = seq_tot_df.copy()

    seq_num_df_a = seq_num_df
    seq_num_df_b = seq_num_df[merge_fields + ['productKey', 'grossSales', 'volume']].copy()

    if within_trans:
        seq_num_df_c = seq_num_df_a.merge(seq_num_df_b, on=merge_fields, suffixes=('', 'From'))
        seq_num_df_c = seq_num_df_c[seq_num_df_c['productKey'] != seq_num_df_c['productKeyFrom']]
    else:
        seq_num_df_b['transSeqNum'] += 1
        seq_num_df_c = seq_num_df_a.merge(seq_num_df_b, on=merge_fields, suffixes=('', 'From'))

    seq_tot_df_c = seq_tot_df_a.merge(
        seq_tot_df_b, on=merge_fields, suffixes=('Transaction', 'FromTransaction')
    )

    df = seq_num_df_c.merge(seq_tot_df_c, on=merge_fields)

    # Calc spend switch
    a = df['grossSales'] / df['grossSalesTransaction']
    b = df['grossSalesFrom'] / df['grossSalesFromTransaction']
    c = df['grossSalesTransaction'] + df['grossSalesFromTransaction']
    df['spendSwitch'] = a * b * c
    return df


def trans_seq_num(df, cols=None):
    df = df.reset_index()
    df = df.drop(['transactionDate'], axis=1)

    # Calculate seq num
    df['transSeqNum'] = df.groupby(
        'customerKey'
    )['transactionKey'].rank(method='dense').astype('i8')

    df = calc_spend_switch(df)
    df = df.sort_values(['customerKey', 'transSeqNum'])
    df = df.set_index('customerKey')
    if cols is not None:
        df = df[cols]
    return df


def for_cann(df, cann_group_key):
    tmp_df = df.head(1)
    result = trans_seq_num(tmp_df)
    meta = [(col, dtype) for col, dtype in result.dtypes.items()]
    cols = [col for col, _ in meta]
    with Timer('Trans Seq num'):
        trans_seq_num_df = df.map_partitions(trans_seq_num, cols=cols, meta=meta).compute()
    print(trans_seq_num_df)


def read_df(cann_group_df):
    glob_string = 'data/2017_01_*.parquet'
    logger.info('Reading data %s' % glob_string)
    df = ddf.read_parquet(glob_string)
    df = filter_and_map_cann_groups(df, cann_group_df)
    df = df.persist()
    df = df.drop(['unitVolume'], axis=1)
    logger.info('Read data')
    return df


def main():
    pd.set_option('display.large_repr', 'truncate'); pd.set_option('display.max_columns', 0)  # noqa
    cann_group_df = make_cann_group_df()
    df = read_df(cann_group_df)
    # No here
    df = df.compute()
    df = df.set_index('customerKey', drop=True)
    df = trans_seq_num(df)
    print(df)
    print(df.columns.tolist())
    return
    """
    return
    df = df[df['productKey'].isin([189])]
    from tabulate import tabulate
    df = df.reset_index(drop=True)
    print(tabulate(df, headers='keys', showindex=False))
    return
    """
    # pd.set_option('display.max_rows', 100)  # noqa
    df = client.persist(df)
    logger.info('Setting index')
    df = df.set_index('customerKey')
    logger.info('repartition')
    df = df.repartition(npartitions=ncores)
    df = client.persist(df)
    for cann_group_key in cann_group_df['cannGroupKey'].unique().tolist():
        print('Filtering Cann Group %s' % cann_group_key)
        for_df = df[df['cannGroupKey'] == cann_group_key]
        for_df = for_df.repartition(npartitions=ncores)
        for_df = for_df.persist()
        print('This df: %s' % (len(df),))
        for_cann(for_df, cann_group_key)
        return


if __name__ == '__main__':
    main()
