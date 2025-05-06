# src/data_processing/preprocessor.py
import pandas as pd
import numpy as np

def preprocess_for_new_schema(df_raw, columns_to_keep, column_rename_map, source_file_info=None):
    current_filename = source_file_info.get('filename', 'N/A') if source_file_info else 'N/A'
    print(f"Starting data preprocessing for new schema (file: {current_filename})...")

    available_columns = [col for col in columns_to_keep if col in df_raw.columns]
    missing_columns = set(columns_to_keep) - set(available_columns)
    if missing_columns:
        print(f"Warning (file: {current_filename}): Following requested columns not found: {missing_columns}")
    if not available_columns:
        raise ValueError(f"No requested columns found in raw data for file: {current_filename}. Check config.columns.keep.")

    df = df_raw[available_columns].copy()

    rename_map_filtered = {k: v for k, v in column_rename_map.items() if k in df.columns}
    df.rename(columns=rename_map_filtered, inplace=True)

    # 타임스탬프, 날짜, 시간 컬럼 생성
    if 'timestamp_epoch' in df.columns:
        df['datetime_temp'] = pd.to_datetime(df['timestamp_epoch'], unit='s', errors='coerce')
        df.drop(columns=['timestamp_epoch'], inplace=True)

        df['date'] = df['datetime_temp'].dt.date.astype(str)
        # 마이크로초 포함하여 HH:MM:SS.ffffff 형식으로 저장
        df['time'] = df['datetime_temp'].dt.time.astype(str) # <--- 수정됨
        df['timestamp'] = df['datetime_temp']
        df.drop(columns=['datetime_temp'], inplace=True)
        print(f"Created 'timestamp', 'date', and full 'time' (HH:MM:SS.ffffff) columns for {current_filename}.")
    else:
        df['timestamp'] = pd.NaT
        df['date'] = np.nan
        df['time'] = np.nan # 마이크로초 포함 형식으로도 NaN 또는 빈 문자열 처리
        print(f"Warning: 'timestamp_epoch' not found. 'timestamp', 'date', 'time' will be NaT/NaN for {current_filename}.")

    string_cols = ['ip_src', 'ip_dst', 'client_id', 'topic', 'payload']
    for col in string_cols:
        if col in df.columns:
            if df[col].dtype != 'object':
                df[col] = df[col].astype(object)
            df[col] = df[col].fillna(np.nan)
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else np.nan)

            if col == 'client_id':
                max_len_client_id = 4096
                df.loc[df[col].notna(), col] = df.loc[df[col].notna(), col].str.slice(0, max_len_client_id)
        else:
            df[col] = np.nan

    numeric_cols = ['tcp_srcport', 'tcp_dstport', 'frame_len', 'mqtt_len', 'msg_type', 'ip_proto']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = np.nan
    # print(f"Processed data types and missing values for {current_filename}.")

    def determine_attack_and_anomaly(row):
        base_attack_type = "Unclassified"
        if source_file_info and 'assumed_attack_type' in source_file_info:
            base_attack_type = source_file_info['assumed_attack_type']
        is_anomaly_flag = 0
        final_attack_type = base_attack_type
        is_mqtt_traffic = pd.notna(row.get('msg_type')) or \
                          (pd.notna(row.get('client_id')) and row.get('client_id') not in ['', 'nan', np.nan])

        if pd.notna(row.get('msg_type')) and row.get('msg_type') == 3: # PUBLISH
            if base_attack_type == 'MQTT_Flood':
                is_anomaly_flag = 1
                final_attack_type = 'MQTT_Flood_Detected'
            elif pd.notna(row.get('mqtt_len')) and row.get('mqtt_len', 0) > 1000:
                 is_anomaly_flag = 1
                 final_attack_type = f"{base_attack_type}_LargePayload"
        if pd.notna(row.get('msg_type')) and row.get('msg_type') == 1: # CONNECT
            if base_attack_type == 'BruteForce_Attempt':
                is_anomaly_flag = 1
                final_attack_type = 'BruteForce_Connect_Attempt'
        if base_attack_type == 'Malformed_Packet':
            if pd.isna(row.get('msg_type')) and pd.notna(row.get('tcp_dstport')) and row.get('tcp_dstport') == 1883:
                is_anomaly_flag = 1
                final_attack_type = 'Malformed_Suspected_NoMsgType'
            elif pd.notna(row.get('msg_type')) and not (0 < row.get('msg_type',0) < 16) :
                is_anomaly_flag = 1
                final_attack_type = 'Malformed_InvalidMsgType'
        if not is_mqtt_traffic and pd.notna(row.get('ip_proto')) and row.get('ip_proto') == 6 : # TCP
            if base_attack_type not in ['Legitimate', 'Unclassified']:
                final_attack_type = f"NonMQTT_{base_attack_type}"
            else:
                final_attack_type = "NonMQTT_General_TCP"
        if base_attack_type == 'Legitimate' and is_anomaly_flag == 0:
            if is_mqtt_traffic:
                 final_attack_type = 'Legitimate_MQTT'
            elif pd.notna(row.get('ip_proto')) and row.get('ip_proto') == 6:
                 final_attack_type = 'Legitimate_NonMQTT_TCP'
            else:
                 final_attack_type = 'Legitimate_NonMQTT_OtherProto'
        return pd.Series([final_attack_type, is_anomaly_flag])

    df[['attack_type', 'is_anomaly']] = df.apply(determine_attack_and_anomaly, axis=1)
    # print(f"Created 'attack_type' and 'is_anomaly' columns for {current_filename}.")

    final_cols_ordered = [
        'timestamp', 'ip_src', 'ip_dst', 'tcp_srcport', 'tcp_dstport',
        'frame_len', 'client_id', 'topic', 'mqtt_len', 'payload',
        'msg_type', 'ip_proto', 'attack_type', 'is_anomaly', 'date', 'time'
    ]
    existing_final_cols = [col for col in final_cols_ordered if col in df.columns]
    df_final = df[existing_final_cols].copy()
    # print(f"Preprocessing for new schema (file: {current_filename}) finished. Shape: {df_final.shape}")
    return df_final
