from src.train_model import load_data, train_random_forest
from src.evaluate import evaluate_model
from src.db_utils import init_db, insert_anomalies
import pandas as pd

# 1. 데이터 로딩
X_train, y_train, X_test, y_test, _ = load_data(
    "data/processed/train70_reduced.csv",
    "data/processed/test30_reduced.csv"
)

# 2. 모델 학습
model = train_random_forest(X_train, y_train)

# 3. 모델 평가 및 예측
preds = model.predict(X_test)

# 4. 이상 탐지 결과 추출 (예: 예측과 실제가 다른 경우)
anomaly_indices = y_test.reset_index(drop=True) != preds
anomaly_df = pd.read_csv("data/processed/test30_reduced.csv").loc[anomaly_indices]
anomaly_df = anomaly_df.copy()
anomaly_df["prediction"] = preds[anomaly_indices.values]
anomaly_df["true_label"] = y_test.reset_index(drop=True)[anomaly_indices.values]

# 5. DB 저장 (수정 버전)
cols_to_insert = [col for col in [
    "mqtt.msgtype", "mqtt.qos", "mqtt.retain", "mqtt.ver", "mqtt.protoname"
] if col in anomaly_df.columns]

cols_to_insert += ["prediction", "true_label"]

insert_anomalies(
    anomaly_df[cols_to_insert].rename(columns={
        "mqtt.qos": "qos",
        "mqtt.msgtype": "msg_type",
        "mqtt.retain": "retain",
        "mqtt.ver": "version",
        "mqtt.protoname": "protocol"
    })
)


# 6. 평가 결과 출력
evaluate_model(model, X_test, y_test, model_name="Random Forest")
