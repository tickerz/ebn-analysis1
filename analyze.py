import time
import csv
import pandas as pd
import numpy as np
import psycopg2 as pg



def process_chunk(chunk):
    answer=chunk.groupby('consignor_state').consginor_state.count()
    return answer

size=5000000

start=time.time()
connection=pg.connect("dbname=ewaybill_test user=postgres")

i=0

for data_chunk in pd.read_sql_query("select * from ewaybill", connection, chunksize=size):
    if(i==0):
        result=process_chunk(data_chunk)
        print("Took %s seconds" %(time.time()-start))
        i=i+1
    else:
        exit()
    del(data_chunk)

result.to_csv('/tmp/op.csv')