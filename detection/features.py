# src/detection/features.py 예시
import pandas as pd

def add_time_window_features(df, window_size='1T'): # 1분 윈도우
    if 'timestamp' not in df.columns or not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        print("Error: 'timestamp' column is missing or not datetime type.")
        return df

    df_sorted = df.sort_values(by='timestamp')
    # ip_src별, 시간 윈도우별 패킷 수
    df_sorted['packets_in_window_by_ip'] = df_sorted.groupby('ip_src')['timestamp'].transform(
        lambda x: x.rolling(window_size).count()
    )
    # ip_src별, 시간 윈도우별 특정 msg_type (예: CONNECT=1) 수
    # 이 부분은 더 복잡하므로, apply나 다른 방식 필요할 수 있음. 여기서는 개념만.
    # df_sorted['connects_in_window_by_ip'] = df_sorted[df_sorted['msg_type']==1].groupby('ip_src')['timestamp'].transform(...)
    return df_sorted
