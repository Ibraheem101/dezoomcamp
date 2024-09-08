#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
import pyarrow.parquet as pq

from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url


    # raw_df = pd.read_parquet('yellow_tripdata_2024-01.parquet')
    # df = raw_df.iloc[:100]
    file_name = 'dataset.parquet'
    os.system(f'wget {url} -O {file_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    parquet_file = pq.ParquetFile(file_name) # Open the Parquet file
    batch_iterator = parquet_file.iter_batches(batch_size=100000) # Create an iterator from the batches
    batch = next(batch_iterator)  # Get the next batch
    df_batch = batch.to_pandas()

    # First batch of data
    df_batch.to_sql(name=table_name, con=engine, if_exists='replace')

    # Remaining batches
    try:
        while True:
            start_time = time()

            batch = next(batch_iterator)  # Get the next batch
            df_batch = batch.to_pandas()
            
            df_batch.to_sql(name=table_name, con=engine, if_exists='append')
            end_time = time()
            print("next batch inserted ..., took %.3f seconds" % (end_time-start_time))
    except StopIteration:
        print("reached the end of last batch")

if __name__ == '__main__':
    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of csv file

    parser = argparse.ArgumentParser(description='Ingest parquet to Postgres')
    parser.add_argument('--user', help='postgres username')
    parser.add_argument('--password', help='postgres password')
    parser.add_argument('--host', help='postgres host')
    parser.add_argument('--port', help='postgres port')
    parser.add_argument('--db', help='postgres database name')
    parser.add_argument('--table_name', help='results written to table')
    parser.add_argument('--url', help='url of parquet file')

    args = parser.parse_args()
    main(args)