# src/data_processing/loader.py
import pandas as pd

def load_raw_data(csv_path, max_rows=None):
    load_message = f"Loading raw data from: {csv_path}"
    if max_rows is not None:
        load_message += f" (max_rows: {max_rows})"
    print(load_message)
    try:
        df_raw = pd.read_csv(csv_path, low_memory=False, nrows=max_rows)
        if max_rows is not None:
            actual_rows_loaded = len(df_raw)
            print(f"Successfully loaded {actual_rows_loaded} rows from '{csv_path}' (requested max: {max_rows}).")
        else:
            print(f"Successfully loaded {len(df_raw)} rows and {len(df_raw.columns)} columns from '{csv_path}'.")
        return df_raw
    except FileNotFoundError:
        print(f"Error: Raw data file not found at {csv_path}")
        raise
    except Exception as e:
        print(f"Error loading raw data from '{csv_path}': {e}")
        raise
