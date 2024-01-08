import gc
import os
import sys

import pandas as pd
from sqlalchemy import create_engine

from minio import Minio
from io import BytesIO

def get_minio_client():
    return Minio(
        "localhost:9400",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

def download_files_from_minio(bucket_name, minio_client):
    objects = minio_client.list_objects(bucket_name)
    for obj in objects:
        if obj.object_name.endswith('.parquet'):
            data = minio_client.get_object(bucket_name, obj.object_name)
            yield obj.object_name, BytesIO(data.read())

def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main() -> None:
    bucket_name = "datalake"  
    minio_client = get_minio_client()

    for file_name, file_data in download_files_from_minio(bucket_name, minio_client):
        parquet_df = pd.read_parquet(file_data, engine='pyarrow')
        clean_column_name(parquet_df)
        if not write_data_postgres(parquet_df):
            del parquet_df
            gc.collect()
            return

        del parquet_df
        gc.collect()


if __name__ == '__main__':
    sys.exit(main())
