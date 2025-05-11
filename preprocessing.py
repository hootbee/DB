import pandas as pd
import numpy as np
from sklearn.utils import shuffle


def cleandata(df):
    to_drop = [
        'frame.time_invalid', 'frame.time_epoch', 'frame.time_relative', 'frame.number',
        'frame.time_delta', 'frame.time_delta_displayed', 'frame.cap_len', 'frame.len',
        'tcp.window_size_value', 'eth.src', 'eth.dst', 'ip.src', 'ip.dst', 'ip.proto',
        'tcp.srcport', 'tcp.dstport', 'tcp.analysis.initial_rtt', 'tcp.stream', 'mqtt.topic',
        'tcp.checksum', 'mqtt.topic_len', 'mqtt.passwd_len', 'mqtt.passwd',
        'mqtt.clientid', 'mqtt.clientid_len', 'mqtt.username', 'mqtt.username_len'
    ]
    df.drop(columns=[col for col in to_drop if col in df.columns], inplace=True)
    return df


def load_and_clean(filepath, label):
    df = pd.read_csv(filepath)
    df.fillna(0, inplace=True)
    df['target'] = label
    return cleandata(df)


def augment_dataset(df, repeat, head_rows):
    augmented = pd.DataFrame()
    for _ in range(repeat):
        augmented = augmented.append(df.head(head_rows), ignore_index=True)
    return augmented


def create_full_dataset():
    legit = load_and_clean('data/raw/legitimate_1w.csv', 'legitimate')
    slowite = load_and_clean('data/raw/slowite.csv', 'slowite')
    malaria = load_and_clean('data/raw/malaria.csv', 'dos')
    malformed = load_and_clean('data/raw/malformed.csv', 'malformed')
    flood = load_and_clean('data/raw/flood.csv', 'flood')
    brute = load_and_clean('data/raw/bruteforce.csv', 'bruteforce')

    full = pd.concat([legit, slowite, malaria, malformed, flood, brute], ignore_index=True)
    full = shuffle(full, random_state=10)
    full.to_csv('data/processed/mqttdataset.csv', index=False)
    print("✅ mqttdataset.csv saved.")


def create_augmented_dataset():
    seed = 7

    legit = load_and_clean('data/raw/legitimate_1w.csv', 'legitimate')
    trainleg = legit.head(7000000)
    testleg = legit.tail(3000000)

    slowite = augment_dataset(load_and_clean('data/raw/slowite.csv', 'slowite'), 250, 8000)
    trainslow = slowite.head(1400000)
    testslow = slowite.tail(600000)

    malaria = augment_dataset(load_and_clean('data/raw/malaria.csv', 'dos'), 15, 130000)
    malaria = malaria.append(load_and_clean('data/raw/malaria.csv', 'dos').head(50000), ignore_index=True)
    trainmalaria = malaria.head(1400000)
    testmalaria = malaria.tail(600000)

    malformed = augment_dataset(load_and_clean('data/raw/malformed.csv', 'malformed'), 200, 10000)
    trainmalformed = malformed.head(1400000)
    testmalformed = malformed.tail(600000)

    flood = augment_dataset(load_and_clean('data/raw/flood.csv', 'flood'), 4000, 500)
    trainflood = flood.head(1400000)
    testflood = flood.tail(600000)

    brute = augment_dataset(load_and_clean('data/raw/bruteforce.csv', 'bruteforce'), 142, 14000)
    brute = brute.append(load_and_clean('data/raw/bruteforce.csv', 'bruteforce').head(12000), ignore_index=True)
    trainbrute = brute.head(1400000)
    testbrute = brute.tail(600000)

    df_train = pd.concat([trainleg, trainmalaria, trainmalformed, trainslow, trainflood, trainbrute])
    df_train = shuffle(df_train, random_state=seed)
    df_train.to_csv('data/processed/train70_augmented_new.csv', index=False)

    df_test = pd.concat([testleg, testbrute, testflood, testmalaria, testmalformed, testslow])
    df_test = shuffle(df_test, random_state=seed)
    df_test.to_csv('data/processed/test30_augmented_new.csv', index=False)

    print("✅ train70_augmented_new.csv and test30_augmented_new.csv saved.")


if __name__ == "__main__":
    create_full_dataset()
    create_augmented_dataset()
