import fastparquet
import pandas as pd
from sqlalchemy import create_engine 

# df = pd.read_parquet('C:/Users/LENOVO/Documents/GitHub/ingestion-data/dataset/yellow_tripdata_2023-01.parquet', engine="pyarrow")

pf = fastparquet.ParquetFile('C:/Users/LENOVO/Documents/GitHub/ingestion-data/dataset/yellow_tripdata_2023-01.parquet')
df = pf.to_pandas()
df['passenger_count'] = df['passenger_count'].astype('int64')
df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(bool)
print(df)
df.info()

# parquet_file = 'C:/Users/LENOVO/Documents/GitHub/ingestion-data/dataset/yellow_tripdata_2023-01.parquet'
# df = pd.read_parquet(parquet_file, engine='fastparquet')
# df = df[['pickup_datetime', 'passenger_count', 'trip_distance', 'total_amount']]
# df = df.dropna()
# dtype = {'passenger_count': 'INTEGER',
#          'store_and_fwd_flag':'BOOLEAN'}

# database_url = 'sqlite:///my_database.db'

# # Use to_sql to write the cleaned DataFrame to the database with specified data types
# df.to_sql(name='yellow_tripdata_cleaned', con=database_url, index=False, if_exists='replace', dtype=dtype)