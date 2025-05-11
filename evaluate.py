# src/evaluate.py
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix, classification_report
import joblib


def evaluate_model(model, X_test, y_test, model_name="model"):
    print(f"\n📊 Evaluation for {model_name}")
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average='weighted')
    matrix = confusion_matrix(y_test, y_pred)

    print(f"Accuracy: {acc:.4f} | F1-score: {f1:.4f}")
    print("Confusion Matrix:")
    print(matrix)
    print("Classification Report:")
    print(classification_report(y_test, y_pred))
    return y_pred


if __name__ == "__main__":
    # 평가용 테스트 데이터 로딩
    df_test = pd.read_csv("data/processed/test30_reduced.csv")
    df_test = df_test.astype("category")
    cat_columns = df_test.select_dtypes(['category']).columns
    df_test[cat_columns] = df_test[cat_columns].apply(lambda x: x.cat.codes)
    X_test = df_test.drop("target", axis=1)
    y_test = df_test["target"]

    # 저장된 모델 불러오기
    rf_model = joblib.load("models/rf_model.pkl")
    evaluate_model(rf_model, X_test, y_test, model_name="Random Forest")

    dt_model = joblib.load("models/dt_model.pkl")
    evaluate_model(dt_model, X_test, y_test, model_name="Decision Tree")

    nb_model = joblib.load("models/nb_model.pkl")
    evaluate_model(nb_model, X_test, y_test, model_name="Naive Bayes")

    gb_model = joblib.load("models/gb_model.pkl")
    evaluate_model(gb_model, X_test, y_test, model_name="Gradient Boost")

    mlp_model = joblib.load("models/mlp_model.pkl")
    evaluate_model(mlp_model, X_test, y_test, model_name="MLP")
