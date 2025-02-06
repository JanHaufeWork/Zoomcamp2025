import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import logging
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    csv_name = './data/output.csv.gz'

    # Ensure the download directory exists
    os.makedirs('./data', exist_ok=True)

    # Download CSV file
    os.system(f'wget {url} -O {csv_name}')

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Initialize connection to database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{table_name}')
    engine.connect()

    # Read data in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    # Create table schema using the first chunk
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df['tpep_pickup_datetime'])
    df.tpep_dropoff_datetime = pd.to_datetime(df['tpep_dropoff_datetime'])
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Write the first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # Process and write remaining chunks
    n = 1
    for df in df_iter:
        t_start = time()
        
        df.tpep_pickup_datetime = pd.to_datetime(df['tpep_pickup_datetime'])
        df.tpep_dropoff_datetime = pd.to_datetime(df['tpep_dropoff_datetime'])
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        logging.info('Time to write chunk %i: %.3f seconds', n, t_end - t_start)
        n += 1


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Load NY taxi data to database')

    parser.add_argument('--user', type=str, help='username for postgres', required=True)
    parser.add_argument('--password', type=str, help='password for postgres', required=True)
    parser.add_argument('--host', type=str, help='host for postgres', required=True)
    parser.add_argument('--port', type=str, help='port for postgres', required=True)
    parser.add_argument('--db', type=str, help='database name for postgres', required=True)
    parser.add_argument('--table_name', type=str, help='name of the table where we will write the results to', required=True)
    parser.add_argument('--url', type=str, help='url of the csv file', required=True)

    args = parser.parse_args()
    
    main(args)
