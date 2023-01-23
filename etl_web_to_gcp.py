from pathlib import Path
import pandas as pd
import pyarrow
from prefect import flow , task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import  S3


@task(retries=3)
def fetch(dataste_url : str) -> pd.DataFrame:
    """Read data from web into pandas"""
    df = pd.read_csv(dataste_url)
    return df 
     

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix Dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f'columns:  {df.dtypes}')
    return df

@task()
def write_local(df : pd.DataFrame , color : str , dataset_file:str) -> Path:
    """Write dataframe out locally as parquet file"""
    path = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(path ,engine='pyarrow' ,compression='gzip')
    return path

@task()
def write_to_s3(path:Path) -> None:
    """Upload local parquet file to gcs"""
    s3_block =  S3.load("etl-s3")
    s3_block.put_directory(f"{path}", path)
    return 


@flow()
def etl_web_to_gcs() -> None:
    """The main etl function"""
    color = 'yellow'
    year = 2021
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean , color , dataset_file)
    write_to_s3(path)

if __name__ == '__main__':
    etl_web_to_gcs() 