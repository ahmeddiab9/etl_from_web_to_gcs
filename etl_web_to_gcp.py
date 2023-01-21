from pathlib import Path
import pandas as pd
from prefect import flow , task
from prefect_gcp.cloud_storage import GcsBucket


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
    path = Path(rf'/data/yellowr/yellow_tripdata_2021-02.parquet')
    df.to_parquet(path , compression='gzip')
    return path

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


if __name__ == '__main__':
    etl_web_to_gcs() 