import os 
from time import time
import pandas as pd
from sqlalchemy import create_engine
import prefect
from prefect import task, Flow


def ingest_data(user , password , host , port , db , table_name ,url):
    if url.endswith('.csv.gz'):
        csv_name = "yellew_tripdata_2021-01.csv.gz"
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")
    # csv_name  = "yellew_tripdata_2021-01.csv.gz"
    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(csv_name , iterator=True ,chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name , con=engine , if_exists="replace")
    df.to_sql(name=table_name , con=engine , if_exists="append")

    while True:
        try: 
            t_start = time()
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name , con=engine , if_exists="append")

            t_end = time()

            print("inserted onther chunck , took %.3f second" % (t_end - t_start))

        except StopIteration:
            print("Finished Ingesting Data into the postgre database")
            break

if __name__ == '__main__':
    user = 'postgres'
    password = 'admin123'
    host = 'localhost'
    port = '5432'
    db = "ny_taxi"
    table_name = 'yellow_taxi_trips'
    csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'

    ingest_data(user , password , host , port , db , table_name , csv_url)

