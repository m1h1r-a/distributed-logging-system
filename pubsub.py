import json
import threading
import time
from datetime import datetime, timedelta

from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    "info_log",
    "warn_log",
    "error_log",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

heartbeat = KafkaConsumer(
    "inventoryHeartbeat",
    "orderHeartbeat",
    "paymentHeartbeat",
    value_deserializer=lambda y: json.loads(y.decode("utf-8")),
)

node_status = {}
TIMEOUT = 9


def receive_heartbeat():
    for message in heartbeat:
        heartbeat_message = message.value
        # print(f"HEARTBEAT: {heartbeat_message}")
        node_id = heartbeat_message.get("node_id")
        timestamp = datetime.now()

        node_status[node_id] = timestamp
        print(f"HEARTBEAT RECEIVED [{node_id}] at [{timestamp}]")


def monitor_nodes():
    while True:
        now = datetime.now()
        for node_id, last_seen in list(node_status.items()):
            time_diff = (now - last_seen).total_seconds()

            if time_diff > TIMEOUT:
                print(f"NODE FAILED: {node_id} - Last seen {time_diff:.1f} seconds ago")
                del node_status[node_id]
        time.sleep(5)


def receive_logs():
    for message in consumer:
        log = message.value

        if log["log_level"] == "INFO":

            pass
            # Connect to Elasticsearch

        else:

            if log["log_level"] == "WARN":
                print("WARNING LOG:")
                print(json.dumps(log, indent=4))
                print()

            elif log["log_level"] == "ERROR":
                print("ERROR LOG:")
                print(json.dumps(log, indent=4))
                print()


if __name__ == "__main__":

    threading.Thread(target=receive_heartbeat).start()
    threading.Thread(target=monitor_nodes).start()
    threading.Thread(target=receive_logs).start()
