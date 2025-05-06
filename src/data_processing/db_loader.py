# src/data_processing/db_loader.py
import pandas as pd

def load_data_to_db(df, table_name, engine):
    if df.empty:
        print(f"No data to load into the database for the current batch.")
        return
    print(f"Loading {len(df)} rows into table: '{table_name}'...")
    try:
        chunk_size = 10000
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=chunk_size)
        print(f"Successfully loaded data into '{table_name}'.")
    except Exception as e:
        print(f"Error loading data into database table '{table_name}': {e}")
        raise
