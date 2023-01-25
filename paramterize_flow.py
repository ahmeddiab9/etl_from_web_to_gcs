# prefect orion start
from pathlib import Path
import pandas as pd
from prefect import flow , task
from prefect_aws.s3 import S3Bucket
from prefect.tasks import task_input_hash
from datetime import timedelta



@task(retries=3 , cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
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

@task(log_prints=True)
# aws Access key => AKIATYWOATBH4UM7NYWB
# aws secretkey => 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
def write_to_s3(path:Path) -> None:
    """Upload local parquet file to s3"""
    s3_bucket_block = S3Bucket.load("etl-s3-bucket")
    s3_bucket_block.upload_from_path(path, 'data/yellow/yellow_tripdata_2021-01.parquet')
    return 


@flow()
def etl_web_to_s3(color : str , year : int , month : int) -> None:
    """The main etl function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean , color , dataset_file)
    write_to_s3(path)
    
@flow()
def etl_parent_flow( months : list[int] = [1,2,3] , year : str = '20121' ,  color : str = 'yellow'):
    for month in months:
        etl_web_to_s3(color,  year, month)
    
if __name__ == '__main__':
    color = 'yellow'
    month = [1 , 2 , 3]
    year = 2021
    etl_parent_flow(month , year , color)

# python paramterize_flow.py
# prefect deployment build ./paramterize_flow.py:etl_parent_flow -n "Parameterized Etl"
# prefect deployment apply etl_parent_flow-deployment.yaml