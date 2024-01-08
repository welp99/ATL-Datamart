import pandas as pd

# Replace this with the path to your Parquet file
file_path = '/Users/welp/Documents/Cours Ingénieur/Archi Décisionelle/TP archi/ATL-Datamart/data/raw/yellow_tripdata_2023-01.parquet'

# Read the Parquet file
df = pd.read_parquet(file_path)

# Display the first few rows of the dataframe
print(df.head())

# Display the dataframe's column names and data types
print(df.dtypes)
