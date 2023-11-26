import pandas as pd

#1
file_path = "C:/Users/LENOVO/Documents/GitHub/ingestion-data/dataset/yellow_tripdata_2020-07.csv"
df = pd.read_csv(file_path)
print(df)

#2
original_columns = df.columns
snake_case_columns = [col.lower().replace(' ', '_') for col in original_columns]
column_mapping = dict(zip(original_columns, snake_case_columns))
df.rename(columns=column_mapping, inplace=True)
print(df)

# 3.1
top_10_passenger_count = df.nlargest(10, 'passenger_count')
print(top_10_passenger_count)

# 3.2
df_multiple_cols = df[["vendorid", "passenger_count", "trip_distance", "payment_type", "fare_amount", "extra", 
                       "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                       "total_amount", "congestion_surcharge"]]
print("selecting multiple columns")
print(df_multiple_cols)


# 4
df['passenger_count'] = df['passenger_count'].fillna(0).astype(int)
df = df.dropna(subset=['passenger_count']).astype({'passenger_count': int})
df['passenger_count'] = df['passenger_count'].astype(float).fillna(0).astype(int)
df_single_col = df["passenger_count"]
print(df_single_col)