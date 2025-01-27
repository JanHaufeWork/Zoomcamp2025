import pandas as pd
from sqlalchemy import create_engine
from time import time


#initialize connection to database
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()


df = pd.read_csv('./data/yellow_tripdata_2021-01.csv', nrows=100)
#df.head()

#convert datetime to datetime type
df.tpep_pickup_datetime = pd.to_datetime(df['tpep_pickup_datetime'])
df.tpep_dropoff_datetime = pd.to_datetime(df['tpep_dropoff_datetime'])

#create ddl to create table in postgres db
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

#write data in chunks to postgres db
df_iter = pd.read_csv('./data/yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000, low_memory=False)
df = next(df_iter)

#create empty table 
df.head(n=0).to_sql('yellow_taxi_data', con=engine, if_exists='replace')

#write first iter
df.to_sql('yellow_taxi_data', con=engine, if_exists='append')

#filling in data
n = 1
for df in df_iter:
    t_start = time()

    df.tpep_pickup_datetime = pd.to_datetime(df['tpep_pickup_datetime'])
    df.tpep_dropoff_datetime = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    df.to_sql('yellow_taxi_data', con=engine, if_exists='append')

    t_end = time()

    print('Time to write chunk %i: %.3f seconds' %(n, t_end - t_start))
    n += 1