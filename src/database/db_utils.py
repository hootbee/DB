# src/database/db_utils.py
from sqlalchemy import create_engine, text

def get_db_engine(db_config):
    """SQLAlchemy DB 엔진을 생성합니다."""
    print("Creating database engine...")
    try:
        engine_url = (
            f"{db_config['type']}+{db_config['connector']}://"
            f"{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['db_name']}"
        )
        engine = create_engine(engine_url)
        with engine.connect() as connection:
            print("Database connection successful.")
        return engine
    except KeyError as e:
        print(f"Error creating database engine: Configuration key {e} not found in config.yaml.")
        raise
    except Exception as e:
        print(f"Error creating database engine: {e}")
        raise

def create_target_table_if_not_exists(engine, db_name, table_name_to_create):
    """지정된 이름으로 테이블이 없으면 생성합니다."""
    create_sql = get_common_table_schema_sql(table_name_to_create)
    print(f"Checking/Creating table '{table_name_to_create}' in database '{db_name}'...")
    with engine.connect() as connection:
        try:
            pass
        except Exception as e:
            print(f"Error selecting database '{db_name}': {e}")
            raise
        try:
            connection.execute(text(create_sql))
            connection.commit()
            print(f"Table '{table_name_to_create}' checked/created successfully.")
        except Exception as e:
            print(f"Error creating table '{table_name_to_create}': {e}")
            raise

def get_common_table_schema_sql(table_name):
     """모든 로그 테이블에 적용될 공통 스키마 SQL을 반환합니다."""
     return f"""
     CREATE TABLE IF NOT EXISTS {table_name} (
         id INT AUTO_INCREMENT PRIMARY KEY,
         timestamp DATETIME NULL,
         ip_src VARCHAR(45) NULL,
         ip_dst VARCHAR(45) NULL,
         tcp_srcport INT NULL,
         tcp_dstport INT NULL,
         frame_len INT NULL,
         client_id VARCHAR(4096) NULL,
         topic VARCHAR(1024) NULL,
         mqtt_len INT NULL,
         payload TEXT NULL,
         msg_type INT NULL,
         ip_proto INT NULL,
         attack_type VARCHAR(100) NULL,
         is_anomaly TINYINT DEFAULT 0,
         date VARCHAR(10) NULL,
         time VARCHAR(15) NULL,        -- <--- 수정됨: VARCHAR(15)로 변경 (HH:MM:SS.ffffff)
         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     );
     """
