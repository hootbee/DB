# src/db_utils.py
import sqlite3
import pandas as pd
import os

def init_db(db_path='data/anomaly_logs.db'):
    """DB 파일과 테이블 초기화"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            client_id TEXT,
            topic TEXT,
            qos INTEGER,
            prediction TEXT,
            true_label TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print(f"✅ anomaly DB initialized at {db_path}")


def insert_anomalies(df: pd.DataFrame, db_path='data/anomaly_logs.db'):
    """이상 탐지 결과를 DB에 저장"""
    if df.empty:
        print("⚠️ No anomaly data to insert.")
        return

    with sqlite3.connect(db_path) as conn:
        df.to_sql('anomalies', conn, if_exists='append', index=False)
    print(f"✅ {len(df)} rows inserted into DB.")


if __name__ == "__main__":
    # 테스트용 샘플 데이터 삽입
    sample_data = pd.DataFrame({
        'timestamp': ['2025-05-11 16:00:00'],
        'client_id': ['clientX'],
        'topic': ['sensor/temp'],
        'qos': [1],
        'prediction': ['dos'],
        'true_label': ['legitimate']
    })

    init_db()
    insert_anomalies(sample_data)
