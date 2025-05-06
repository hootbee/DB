# src/utils/config_loader.py (DB_HOST 필수로 처리 - 이전 답변과 동일)
import yaml
import os
from dotenv import load_dotenv

def load_and_enrich_config(config_path='config/config.yaml'):
    """
    YAML 설정 파일을 로드하고, .env 파일의 환경 변수로 주요 DB 설정을 덮어쓰거나 채웁니다.
    DB_USER, DB_PASSWORD, DB_HOST는 필수 환경 변수입니다.
    """
    # .env 파일 로드
    load_dotenv()

    # 필수 환경 변수 목록 및 값 가져오기
    db_user_env = os.getenv("DB_USER")
    db_password_env = os.getenv("DB_PASSWORD")
    db_host_env = os.getenv("DB_HOST") # DB_HOST도 필수로 가져옴

    # 필수 환경 변수 누락 확인
    missing_vars = []
    if not db_user_env:
        missing_vars.append("DB_USER")
    if not db_password_env:
        missing_vars.append("DB_PASSWORD")
    if not db_host_env: # DB_HOST도 필수로 확인
        missing_vars.append("DB_HOST")

    if missing_vars:
        raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

    # YAML 파일 로드
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {config_path}")

    config = None # 초기화
    with open(config_path, 'r', encoding='utf-8') as f:
        try:
            config = yaml.safe_load(f)
            if config is None: # 빈 YAML 파일의 경우
                config = {}
            print("Configuration loaded successfully from YAML.")

            # DB 설정 섹션이 없으면 생성
            if 'database' not in config or not isinstance(config['database'], dict):
                config['database'] = {}

            # 환경 변수로 필수 DB 설정 업데이트 (YAML 파일의 값을 덮어씀)
            config['database']['user'] = db_user_env
            config['database']['password'] = db_password_env
            config['database']['host'] = db_host_env # YAML 값 대신 환경 변수 값 사용

            # (선택적) DB_PORT 처리: 환경 변수 > YAML > 기본값 3306
            yaml_port_str = None
            if 'database' in config and isinstance(config['database'], dict):
                yaml_port_str = str(config['database'].get('port', ''))

            try:
                env_db_port_str = os.getenv("DB_PORT")
                if env_db_port_str is not None:
                    config['database']['port'] = int(env_db_port_str)
                elif yaml_port_str and yaml_port_str.isdigit():
                    config['database']['port'] = int(yaml_port_str)
                else:
                    config['database']['port'] = 3306
            except ValueError:
                print(f"Warning: DB_PORT 또는 YAML의 port 값이 유효한 숫자가 아닙니다. 기본 포트 3306을 사용합니다.")
                config['database']['port'] = 3306


            # type, connector, db_name은 YAML에서 그대로 사용한다고 가정
            # (필요시 이들도 환경 변수로 관리 가능)
            if 'type' not in config['database']: config['database']['type'] = 'mysql'
            if 'connector' not in config['database']: config['database']['connector'] = 'mysqlconnector'
            if 'db_name' not in config['database']:
                config['database']['db_name'] = os.getenv("DB_NAME")
                if not config['database']['db_name']:
                     raise ValueError("database.db_name이 YAML에 정의되지 않았고 DB_NAME 환경 변수도 없습니다.")


            print("Database configuration enriched/validated with environment variables and defaults.")
            return config

        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {config_path} - {e}")
            raise
        except ValueError as e: # 위에서 raise된 ValueError 포함
            print(f"Configuration Error: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred during config loading: {e}")
            raise

# 이전 load_config 함수 이름 유지를 원한다면
def load_config(config_path='config/config.yaml'):
    return load_and_enrich_config(config_path)
