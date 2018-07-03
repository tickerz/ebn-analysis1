import time
import pandas as pd
import psycopg2 as pg
import csv

start=time.time()
connection=pg.connect("dbname='ewaybill_test' user='postgres' port='5433'")

def process_chunk(chunk):
    answer=chunk.groupby('consignor_state').consignor_state.count()
    return answer

chunk_size=1000000
offset=1000000

while True:
    df=pd.read_sql_query("SELECT * FROM ewaybill limit %d offset %d order by ID" % (chunk_size,offset))
    if offset == 1000000:
        result=process_chunk(df)
    else:
        result=result.append(process_chunk(df))
    del(df)
    chunk_size+=chunk_size
    offset+=offset
    if (offset>25000000):
        break

result.to_csv('/tmp/op.csv')