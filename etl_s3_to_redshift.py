# prefect orion start
from pathlib import Path
import pandas as pd
import psycopg2
from prefect import flow , task
from prefect_aws.s3 import S3Bucket
from prefect_aws import AwsCredentials
import pyarrow.parquet as pq
import s3fs
s3 = s3fs.S3FileSystem()
from red_panda import RedPanda
from sqlalchemy import create_engine

# aws_credentials_block = AwsCredentials.load("etl-from-web-to-s3")


@task()
def extract_from_redshift(color:str , year:int , month:int) -> pd.DataFrame:
    s3_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    s3_bucket_block = S3Bucket.load("etl-s3-bucket")
    s3_bucket_block.get_directory()
    return Path(s3_path)

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning and transformation"""
    df = pd.read_parquet(Path(path))
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0 , inplace=True)
    print(f"after: missing passenger count: {df['passenger_count'].isna().sum()}")
    print(df.head())
    return df
    

@task()
def write_redshift(df : pd.DataFrame) -> None:
    """Write data to Redshift"""
    #name_space etl-from-s3-zoom
    # https://stackoverflow.com/questions/38402995/how-to-write-data-to-redshift-that-is-a-result-of-a-dataframe-created-in-python
    conn = create_engine('postgresql+psycopg2://awsuser:Admin123@redshift-cluster-1.cltns8ijhpx4.us-east-1.redshift.amazonaws.com:5439/dev')
    df.to_sql('rides', conn, index=False, if_exists='replace' , chunksize = 100000)
    print('successfully insert to redshift')




@flow()
def etl_ws3_to_redshift() -> None:
    """The main etl flow to load into redshift"""
    color = 'yellow'
    year = 2021
    month = 1
    
    path = extract_from_redshift(color , year , month)
    df = transform(path)
    write_redshift(df)


    
if __name__ == '__main__':
    etl_ws3_to_redshift() 