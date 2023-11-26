import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, func, select
from sqlalchemy.types import BigInteger, String, JSON, DateTime, Boolean, Float, Integer

dbname = 'mydb'
user = 'postgres'
password = 'admin'
host = 'localhost'
port = '5432'

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

def get_manipulate_data(df):
    df.dropna(inplace= True)
    df['VendorID'] = df['VendorID'].astype('int8')
    df['passenger_count'] = df['passenger_count'].astype('int8')
    df['RatecodeID'] = df['RatecodeID'].astype('int8')
    df['PULocationID'] = df['PULocationID'].astype('int8')
    df['DOLocationID'] = df['DOLocationID'].astype('int8')
    df['payment_type'] = df['payment_type'].astype('int8')
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace(['N', 'Y'], [False, True])
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('bool')
    return df

df = pd.read_parquet('C:/Users/LENOVO/Documents/GitHub/ingestion-data/dataset/yellow_tripdata_2023-01.parquet',
                      engine='fastparquet')

clean_data = get_manipulate_data(df)

data_types = {
    'VendorID': BigInteger,
    'tpep_pickup_datetime': DateTime,
    'tpep_dropoff_datetime': DateTime,
    'passenger_count': BigInteger,
    'trip_distance': Float,
    'RatecodeID': BigInteger,
    'store_and_fwd_flag': Boolean,
    'PULocationID': BigInteger,
    'DOLocationID': BigInteger,
    'payment_type': BigInteger,
    'fare_amount': Float,
    'extra': Float,
    'mta_tax': Float,
    'tip_amount': Float,
    'tolls_amount': Float,
    'improvement_surcharge': Float,
    'total_amount': Float,
    'congestion_surcharge': Float,
    'airport_fee': Float
}

# for col, dtype in data_types.items():
#     clean_data[col] = clean_data[col].astype(dtype)

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

table_name = 'alterra_demo'

clean_data.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype=data_types)

# metadata = MetaData()
# metadata.reflect(bind=engine)
# table = metadata.tables['alterra_demo']

# row_count = engine.execute(table.count()).scalar()

# print(f"Number of rows ingested: {row_count}")

# import pandas as pd
# from sqlalchemy import MetaData, create_engine, func, select
# from sqlalchemy.dialects.postgresql import INTEGER, TIMESTAMP, BOOLEAN, FLOAT
# from datetime import datetime

# dbname = 'mydb'
# user = 'postgres'
# password = 'admin'
# host = 'localhost'
# port = '5432'

# engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

# def get_manipulate_data(df):
#     df.dropna(inplace=True)
#     df['VendorID'] = df['VendorID'].astype('int8')
#     df['passenger_count'] = df['passenger_count'].astype('int8')
#     df['RatecodeID'] = df['RatecodeID'].astype('int8')
#     df['PULocationID'] = df['PULocationID'].astype('int8')
#     df['DOLocationID'] = df['DOLocationID'].astype('int8')
#     df['payment_type'] = df['payment_type'].astype('int8')
#     df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace(['N', 'Y'], [False, True])
#     df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('bool')
#     return df

# df = pd.read_parquet('C:/Users/LENOVO/Documents/GitHub/ingestion-data/dataset/yellow_tripdata_2023-01.parquet', engine='fastparquet')

# clean_data = get_manipulate_data(df)

# data_types = {
#     'VendorID': INTEGER(),
#     'tpep_pickup_datetime': TIMESTAMP(),
#     'tpep_dropoff_datetime': TIMESTAMP(),
#     'passenger_count': INTEGER(),
#     'trip_distance': FLOAT(),
#     'RatecodeID': INTEGER(),
#     'store_and_fwd_flag': BOOLEAN(),
#     'PULocationID': INTEGER(),
#     'DOLocationID': INTEGER(),
#     'payment_type': INTEGER(),
#     'fare_amount': FLOAT(),
#     'extra': FLOAT(),
#     'mta_tax': FLOAT(),
#     'tip_amount': FLOAT(),
#     'tolls_amount': FLOAT(),
#     'improvement_surcharge': FLOAT(),
#     'total_amount': FLOAT(),
#     'congestion_surcharge': FLOAT(),
#     'airport_fee': FLOAT()
# }
# sqlalchemy_types = {
#     'int8': INTEGER(),
#     'bool': BOOLEAN(),
#     'datetime64[ns]': TIMESTAMP(),
#     'float64': FLOAT(),
# }

# for col, dtype in data_types.items():
#     clean_data[col] = clean_data[col].astype(str(dtype.python_type))

#     if dtype.python_type == datetime:
#         clean_data[col] = pd.to_datetime(clean_data[col])
#     else:
#         clean_data[col] = clean_data[col].astype(str(dtype.python_type))

# table_name = 'alterra_demo'

# clean_data.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype=sqlalchemy_types)

# table_name = 'alterra_demo'
# metadata = MetaData()
# metadata.reflect(bind=engine)
# table = metadata.tables[table_name]

# with engine.connect() as connection:
#     count_query = select([func.count()]).select_from(table)
#     row_count = connection.execute(count_query).scalar()

# print(f"Number of rows ingested: {row_count}")