# src/train_model.py
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.neural_network import MLPClassifier
#from tensorflow.keras.models import Sequential
#from tensorflow.keras.layers import Dense
#from tensorflow.keras.callbacks import EarlyStopping
from sklearn.preprocessing import StandardScaler
import joblib
import time


def load_data(train_path, test_path):
    df_train = pd.read_csv(train_path)
    df_test = pd.read_csv(test_path)

    df_train = df_train.astype("category")
    df_test = df_test.astype("category")

    for df in [df_train, df_test]:
        cat_columns = df.select_dtypes(['category']).columns
        df[cat_columns] = df[cat_columns].apply(lambda x: x.cat.codes)

    X_train = df_train.drop("target", axis=1)
    y_train = df_train["target"]
    X_test = df_test.drop("target", axis=1)
    y_test = df_test["target"]

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    return X_train_scaled, y_train, X_test_scaled, y_test, scaler


def train_random_forest(X_train, y_train):
    print("Training: Random Forest")
    model = RandomForestClassifier(random_state=42, verbose=1, n_jobs=-1)
    model.fit(X_train, y_train)
    joblib.dump(model, "models/rf_model.pkl")
    return model


def train_decision_tree(X_train, y_train):
    print("Training: Decision Tree")
    model = DecisionTreeClassifier()
    model.fit(X_train, y_train)
    joblib.dump(model, "models/dt_model.pkl")
    return model


def train_naive_bayes(X_train, y_train):
    print("Training: Naive Bayes")
    model = GaussianNB()
    model.fit(X_train, y_train)
    joblib.dump(model, "models/nb_model.pkl")
    return model


def train_gradient_boost(X_train, y_train):
    print("Training: Gradient Boost")
    model = GradientBoostingClassifier(n_estimators=50, random_state=42, verbose=1)
    model.fit(X_train, y_train)
    joblib.dump(model, "models/gb_model.pkl")
    return model


def train_mlp(X_train, y_train):
    print("Training: Multi-layer Perceptron")
    model = MLPClassifier(max_iter=130, batch_size=1000, alpha=1e-4,
                          activation='relu', solver='adam', verbose=10, random_state=42)
    model.fit(X_train, y_train)
    joblib.dump(model, "models/mlp_model.pkl")
    return model


# def train_keras_nn(X_train, y_train, X_test, y_test):
#     print("Training: Keras Neural Network")
#     model = Sequential()
#     model.add(Dense(50, input_dim=X_train.shape[1], kernel_initializer='normal', activation='relu'))
#     model.add(Dense(30, kernel_initializer='normal', activation='relu'))
#     model.add(Dense(20, kernel_initializer='normal'))
#     model.add(Dense(6, activation='softmax'))
#     model.compile(loss='sparse_categorical_crossentropy', optimizer='adam', metrics=['accuracy'])

#     monitor = EarlyStopping(monitor='val_loss', patience=5)
#     model.fit(X_train, y_train, validation_data=(X_test, y_test),
#               callbacks=[monitor], verbose=2, epochs=200, batch_size=1000)
#     model.save("models/keras_nn_model.h5")
#     return model


if __name__ == "__main__":
    start = time.time()
    X_train, y_train, X_test, y_test, scaler = load_data("data/processed/train70_reduced.csv", "data/processed/test30_reduced.csv")
    joblib.dump(scaler, "models/scaler.pkl")

    # 선택적으로 원하는 모델 학습 실행
    rf_model = train_random_forest(X_train, y_train)
    dt_model = train_decision_tree(X_train, y_train)
    nb_model = train_naive_bayes(X_train, y_train)
    gb_model = train_gradient_boost(X_train, y_train)
    mlp_model = train_mlp(X_train, y_train)
    #keras_model = train_keras_nn(X_train, y_train, X_test, y_test)

    print("✅ All models trained and saved.")
