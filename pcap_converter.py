import pyshark
import pandas as pd
import os

def convert_pcap_to_csv(pcap_path, csv_path, packet_limit=None):
    """
    PCAP 파일을 CSV로 변환
    - MQTT 패킷에서 주요 필드만 추출
    - packet_limit: 분석할 최대 패킷 수
    """
    print(f"📦 Converting: {pcap_path}")
    cap = pyshark.FileCapture(pcap_path, display_filter="mqtt")

    records = []
    count = 0

    for packet in cap:
        try:
            mqtt_layer = packet.mqtt
            frame_info = packet.frame_info

            record = {
                "timestamp": packet.sniff_time.strftime("%Y-%m-%d %H:%M:%S"),
                "client_id": getattr(mqtt_layer, "clientid", ""),
                "topic": getattr(mqtt_layer, "topic", ""),
                "qos": int(getattr(mqtt_layer, "qos", 0)),
                "msg_type": getattr(mqtt_layer, "msgtype", ""),
                "length": int(getattr(frame_info, "cap_len", 0)),
            }
            records.append(record)

            count += 1
            if packet_limit and count >= packet_limit:
                break
        except Exception as e:
            print(f"⚠️ Skipped a packet due to error: {e}")
            continue

    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"✅ Saved to {csv_path} ({len(df)} rows)")


if __name__ == "__main__":
    convert_pcap_to_csv("data/pcap/bruteforce.pcapng", "data/raw/bruteforce.csv", packet_limit=1000)
