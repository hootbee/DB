# run_pipeline.py
import time
import sys
import os

project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.config_loader import load_config
from src.database.db_utils import get_db_engine, create_target_table_if_not_exists
from src.data_processing.loader import load_raw_data
from src.data_processing.preprocessor import preprocess_for_new_schema
from src.data_processing.db_loader import load_data_to_db

def main():
    overall_start_time = time.time()
    print("===============================================")
    print(" Starting MQTT Data Pipeline (ClientID 4096, Time Trimmed, Multi-Table, Row Limit) ") # 제목 업데이트
    print("===============================================")

    try:
        print("\n--- Step 1: Loading Configuration ---")
        config = load_config()
        db_config = config['database']
        data_config = config['data']
        columns_config = config['columns']

        print("\n--- Step 2: Initializing Database Connection ---")
        engine = get_db_engine(db_config)

        if 'file_list' not in data_config or not data_config['file_list']:
            print("Error: 'data.file_list' is missing or empty in config.yaml.")
            return

        base_dir = data_config.get('base_dir', 'data/raw/')
        if not os.path.isdir(base_dir):
            print(f"Error: Base data directory '{base_dir}' not found.")
            return

        total_rows_processed_all_files = 0
        failed_files = []

        row_limits_per_file = {
            'legitimate_1w.csv': 100000
        }

        for file_info in data_config['file_list']:
            if not isinstance(file_info, dict) or 'filename' not in file_info or 'target_table' not in file_info:
                print(f"Warning: Invalid entry in file_list: {file_info}. Skipping.")
                failed_files.append(str(file_info))
                continue

            filename = file_info['filename']
            target_table_name = file_info['target_table']
            assumed_attack_type = file_info.get('assumed_attack_type', 'Unknown')
            full_csv_path = os.path.join(base_dir, filename)

            file_process_start_time = time.time()
            print(f"\n>>> Processing file: {filename} (Target Table: {target_table_name}, Assumed Type: {assumed_attack_type})")

            try:
                print(f"--- Ensuring table '{target_table_name}' exists ---")
                create_target_table_if_not_exists(engine, db_config['db_name'], target_table_name) # 각 테이블 생성
            except Exception as e:
                print(f"Error creating/checking table '{target_table_name}': {e}. Skipping file {filename}.")
                failed_files.append(filename)
                continue

            current_file_source_info = {
                'filename': filename,
                'assumed_attack_type': assumed_attack_type
            }
            current_row_limit = row_limits_per_file.get(filename)

            try:
                df_raw = load_raw_data(full_csv_path, max_rows=current_row_limit)

                print(f"--- Preprocessing Data for New Schema ('{filename}') ---")
                df_processed = preprocess_for_new_schema(
                    df_raw,
                    columns_config['keep'],
                    columns_config['rename'],
                    source_file_info=current_file_source_info
                )

                print(f"--- Loading Processed Data from '{filename}' into Table '{target_table_name}' ---")
                if not df_processed.empty:
                    load_data_to_db(df_processed, target_table_name, engine)
                    total_rows_processed_all_files += len(df_processed)
                    print(f"Successfully processed and loaded {len(df_processed)} rows from '{filename}' into '{target_table_name}'.")
                else:
                    print(f"No data to load from '{filename}' into '{target_table_name}' (empty dataframe).")

            except FileNotFoundError:
                print(f"Error: Data file not found at '{full_csv_path}'. Skipping this file.")
                failed_files.append(filename)
            except ValueError as e:
                 print(f"ValueError during processing of file '{filename}': {e}. Skipping this file.")
                 failed_files.append(filename)
            except Exception as e:
                print(f"An unexpected error occurred while processing file '{filename}': {e}. Skipping this file.")
                failed_files.append(filename)
            finally:
                file_process_end_time = time.time()
                print(f"<<< Finished processing file: {filename} in {file_process_end_time - file_process_start_time:.2f} seconds.")

        overall_end_time = time.time()
        print("\n===============================================")
        print(f"All files processed. Total rows loaded to DB (across all tables): {total_rows_processed_all_files}")
        if failed_files:
            print(f"Warning: The following files failed to process or were skipped: {', '.join(failed_files)}")
        print(f"Total pipeline execution time: {overall_end_time - overall_start_time:.2f} seconds.")
        print("===============================================")

    except FileNotFoundError as e: # 주로 config 파일 로드 실패
        print(f"\nPipeline failed at initialization: {e}")
    except Exception as e: # 기타 초기화 단계 오류
        print(f"\nAn unexpected error occurred during pipeline initialization: {e}")

if __name__ == "__main__":
    main()
