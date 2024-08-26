#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pyarrow.parquet as pq

from time import time
from sqlalchemy import create_engine

raw_df = pd.read_parquet('yellow_tripdata_2024-01.parquet')

df = raw_df.iloc[:100]

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

parquet_file = pq.ParquetFile('yellow_tripdata_2024-01.parquet') # Open the Parquet file
batch_iterator = parquet_file.iter_batches(batch_size=100000) # Create an iterator from the batches
batch = next(batch_iterator)  # Get the next batch
df_batch = batch.to_pandas()

# First batch of data
df_batch.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# Remaining batches
try:
    while True:
        start_time = time()

        batch = next(batch_iterator)  # Get the next batch
        df_batch = batch.to_pandas()
        
        df_batch.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        end_time = time()
        print("next batch inserted ..., took %.3f seconds" % (end_time-start_time))
except StopIteration:
    print("reached the end of last batch")