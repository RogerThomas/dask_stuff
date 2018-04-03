#!/usr/bin/env python
import random

import numpy as np
import pandas as pd
from dask.distributed import Client

from common import Config, Timer, df_mem_in_mb, get_args, get_data_client, logger


def make_cann_group_df(num_products=25):
    random.seed(10)
    cgs = [(1, 'Nestle'), (2, 'Cadbury'), (3, 'Other')]
    cann_group_data = []
    current_product_keys = set()
    for _ in range(num_products):
        while True:
            product_key = random.randint(0, Config.num_products)
            if product_key not in current_product_keys:
                current_product_keys.add(product_key)
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
    df['cannGroupKey'] = df['productKey'].map(cann_group_df['cannGroupKey'])
    df = df[df['cannGroupKey'].isin(cann_group_df['cannGroupKey'].unique())].copy()
    return df


def trans_seq_tot(df):
    df = df.copy()
    df['productCount'] = np.ones(len(df), dtype='u1')
    df = df.groupby(
        ['customerKey', 'transSeqNum'], as_index=False
    )['grossSales', 'netSales'].sum()
    return df


def calc_spend_switch(seq_num_df, within_trans=False):
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


def calculate_switching(df):
    tmp_df = df.head(0)
    result = trans_seq_num(tmp_df)
    meta = [(col, dtype) for col, dtype in result.dtypes.items()]
    cols = [col for col, _ in meta]
    with Timer('Trans Seq num'):
        trans_seq_num_df = df.map_partitions(trans_seq_num, cols=cols, meta=meta).compute()
    print(trans_seq_num_df)


def read_df(args, product_keys):
    d_cli = get_data_client(args)
    with Timer('Read'):
        df = d_cli.read_df()
        df = df[df['productKey'].isin(product_keys)]
        df = df.persist()
        logger.info('Read data: %s rows, %s mb' % (len(df), df_mem_in_mb(df).compute()))
    return df


def main():
    args = get_args()
    client = Client('127.0.0.1:8786')
    ncores = sum(client.ncores().values())
    pd.set_option('display.large_repr', 'truncate'); pd.set_option('display.max_columns', 0)  # noqa pd.set_option('display.max_rows', 1000)  # noqa
    cann_group_df = make_cann_group_df(num_products=100)
    df = read_df(args, cann_group_df['productKey'])
    logger.info('Setting index')
    df = df.set_index('customerKey', drop=True)
    logger.info('Repartitioning')
    df = df.repartition(npartitions=ncores)
    logger.info('Mapping Cann Group')
    df['cannGroupKey'] = df['productKey'].map(cann_group_df['cannGroupKey'])
    logger.info('Persisting')
    df = client.persist(df)
    logger.info('Cann Groups')
    for cann_group_key in cann_group_df['cannGroupKey'].unique().tolist():
        print('Filtering Cann Group %s' % cann_group_key)
        cann_df = df[df['cannGroupKey'] == cann_group_key]
        print('This df: %s' % (len(cann_df),))
        with Timer('%s' % (cann_group_key,)):
            calculate_switching(cann_df)
        return


if __name__ == '__main__':
    main()
