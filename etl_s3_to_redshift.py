# prefect orion start
from pathlib import Path
import pandas as pd
from prefect import flow , task
from prefect_aws.s3 import S3Bucket
from prefect_aws import AwsCredentials
import pyarrow.parquet as pq
import s3fs
s3 = s3fs.S3FileSystem()
from red_panda import RedPanda

# aws_credentials_block = AwsCredentials.load("etl-from-web-to-s3")


@task()
def extract_from_redshift(color:str , year:int , month:int) -> pd.DataFrame:
    s3_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    s3_bucket_block = S3Bucket.load("etl-from-web-to-s3")
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
    # user admin , password Admin123
    # Workgroup name etl-from-s3
    # Namespace name etl-from-s3
    # Database name dev
    redshift_conf = {
    "user": "admin",
    "password": "Admin123",
    "dbname": "dev",
    }
    aws_conf = {
    "aws_access_key_id": "AKIATYWOATBH4UM7NYWB",
    "aws_secret_access_key": "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",
    }
    rp = RedPanda(redshift_conf, aws_conf)
    rp = RedPanda(redshift_conf, aws_conf)
    s3_bucket = "prefect-etl-to-s3"
    s3_file_name = "prefect-etl-to-s3\data\yellow\yellow_tripdata_2021-01.parquet" # optional, randomly generated if not provided
    rp.df_to_redshift(df, "table_name", bucket=s3_bucket, path=s3_file_name, append=False)
    return

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