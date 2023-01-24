import pandas as pd


df = pd.read_parquet("data/yellow/yellow_tripdata_2021-01.parquet")
print(df.head())
