#!/usr/bin/env python
import random

import numpy as np
import pandas as pd

from common import Config, Timer, df_mem_in_mb, get_args, logger, get_data_client


class DFMaker:
    def __init__(self):
        self._current_transaction_key = 1
        self._sales = (np.arange(0, 1000) / 100).astype(np.float32).round(2)

    @staticmethod
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

    def _make_df(self):
        n = random.randint(Config.min_transactions, Config.max_transactions)
        baskets = self.generate_baskets(n)
        transaction_keys = np.arange(
            self._current_transaction_key, self._current_transaction_key + len(baskets)
        )
        self._current_transaction_key += len(baskets)

        def repeat_arr(min_val, max_val):
            arr = np.random.randint(min_val, max_val, len(baskets), dtype=np.int32)
            arr = np.repeat(arr, baskets)
            return arr

        df = pd.DataFrame(dict(
            transactionKey=np.repeat(transaction_keys, baskets),
            productKey=np.random.randint(1, Config.num_products + 1, n, dtype=np.int32),
            storeKey=repeat_arr(1, Config.num_stores + 1),
            customerKey=repeat_arr(1, Config.num_customers + 1),
            grossSales=np.random.choice(self._sales, n),
        ))
        df['netSales'] = df['grossSales']
        return df

    def __call__(self):
        with Timer('Make df'):
            df = self._make_df()
        return df


def group_and_sum_transactions(df):
    df['volume'] = 1
    df['unitVolume'] = 1
    sum_fields = ['grossSales', 'netSales', 'unitVolume', 'volume']
    gb_fields = ['customerKey', 'productKey', 'transactionKey']
    df = df.groupby(gb_fields, as_index=False)[sum_fields].sum()
    return df


def make_data(args):
    ds_cli = get_data_client(args)
    metadata = {'num_rows': 0, 'memory_mb': 0, 'memory_gb': 0}
    df_maker = DFMaker()
    for i, date in enumerate(Config.dates):
        logger.info(date)
        df = df_maker()
        df = group_and_sum_transactions(df)
        df['transactionDate'] = date
        with Timer('Storing df'):
            ds_cli.store_df(df, date_to_fname(date))
        memory_mb = df_mem_in_mb(df)
        logger.info('DF Size: %s Mem MB: %s' % (len(df), memory_mb))
        metadata['num_rows'] += len(df)
        metadata['memory_mb'] += memory_mb
        metadata['memory_gb'] += memory_mb / 1000
        ds_cli.store_obj_as_json(metadata, 'metadata')
        logger.info(metadata)


def date_to_fname(date):
    return str(date.date()).replace('-', '_')


def main():
    args = get_args()
    make_data(args)


if __name__ == '__main__':
    main()
