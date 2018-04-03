import argparse
import json
import logging
import os
import sys
import time

import boto3
import dask.dataframe as ddf
import pandas as pd
import s3fs

root_path = os.path.abspath(os.path.dirname(__file__))
start_date = '01/01/2017'


class ConfigSmall:
    num_products = 6000
    num_stores = 100
    num_customers = 100_000
    min_transactions = 150_000
    max_transactions = 200_000
    dates = pd.date_range(start=start_date, periods=365).tolist()
    min_basket_size = 1
    max_basket_size = 50


class ConfigLarge:
    num_products = 60000
    num_stores = 1000
    num_customers = 10_000_000
    min_transactions = 15_000_000
    max_transactions = 20_000_000
    dates = pd.date_range(start=start_date, periods=365).tolist()
    min_basket_size = 1
    max_basket_size = 50


Config = ConfigSmall


def get_default_logger(name, _cache={}):
    try:
        logger = _cache[name]
    except KeyError:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        _cache[name] = logger
    return logger


logger = get_default_logger('default')


def get_config():
    path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(path) as fh:
        config = json.load(fh)
    return config


config = get_config()


def join_with_root_path(*file_names):
    full_path = os.path.join(root_path, *file_names)
    return full_path


def set_aws_creds():
    aws_creds = config['AWS']
    os.environ['AWS_ACCESS_KEY_ID'] = aws_creds['id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_creds['key']


def df_mem_in_mb(df):
    memory_mb = df.memory_usage(index=True).sum() / (1000 * 1000)
    return memory_mb


class Timer:
    def __init__(self, msg):
        self._msg = msg

    def __enter__(self):
        self._t1 = time.time()
        return self

    def __exit__(self, error_type, error_value, tb):
        if error_type is not None:
            raise
        logger.info('%s took: %s' % (self._msg, time.time() - self._t1))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--compression', default='gzip', required=False, choices=('brotli', 'snappy', 'gzip'),
    )
    parser.add_argument(
        '--location', default='local', required=False, choices=('s3', 'local'),
    )
    args = parser.parse_args()
    return args


class DataClient:
    def __init__(self, compression):
        self._bucket = 'switching-data'
        self._compression = compression

    def _to_parquet(self, df, path_or_fh):
        df.to_parquet(path_or_fh, engine='pyarrow', compression=self._compression)


class S3DataClient(DataClient):
    def __init__(self, compression):
        super().__init__(compression)
        set_aws_creds()
        self._cli = boto3.client('s3')
        self._cli.put_object(Bucket=self._bucket, Body='', Key='%s/' % compression)
        self._s3fs = s3fs.S3FileSystem(anon=False)

    def store_df(self, df, key, folder='trans_product'):
        path = '%s/%s/%s.parquet' % (self._bucket, folder, key)
        with self._s3fs.open(path, 'wb') as fh:
            self._to_parquet(df, fh)

    def store_obj_as_json(self, obj, key, folder='trans_product'):
        path = '%s/%s/%s.json' % (self._bucket, folder, key)
        with self._s3fs.open(path, 'wb') as fh:
            json_str = json.dumps(obj, indent=4, sort_keys=True)
            fh.write(json_str.encode('utf8'))

    def read_df(self):
        file_glob = '2017*.parquet'
        file_glob = '2017_01_*.parquet'
        glob_string = 's3://%s%s/trans_product/%s' % (
            self._bucket, self._compression, file_glob
        )
        logger.info('Reading data %s' % glob_string)
        df = ddf.read_parquet(glob_string, engine='pyarrow')
        return df


class LocalDataClient(DataClient):
    def _get_path(self, folder, fname):
        path = join_with_root_path(self._bucket, self._compression, folder, fname)
        return path

    def store_df(self, df, key, folder='trans_product'):
        path = join_with_root_path(self._bucket, self._compression, folder, '%s.parquet' % key)
        self._to_parquet(df, path)

    def store_obj_as_json(self, obj, key, folder='trans_product'):
        with open(self._get_path(folder, '%s.json' % key), 'w') as fh:
            json.dump(obj, fh)

    def read_df(self):
        glob_string = self._get_path('trans_product', '2017_01*.parquet')
        logger.info('Reading data %s' % glob_string)
        df = ddf.read_parquet(glob_string, engine='pyarrow')
        return df


def get_data_client(args):
    ds_cli = LocalDataClient(args.compression)
    return ds_cli
