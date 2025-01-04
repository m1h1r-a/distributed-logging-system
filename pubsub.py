import json
import logging
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

log_file_path = "/home/pes2ug22cs311/distributedlogging/application_logs.log"
logging.basicConfig(
    filename=log_file_path,
    filemode="a",
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
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
            logging.info(json.dumps(log))

        else:

            if log["log_level"] == "WARN":
                print("WARNING LOG:")
                print(json.dumps(log, indent=4))
                print()
                logging.warning(json.dumps(log))

            elif log["log_level"] == "ERROR":
                print("ERROR LOG:")
                print(json.dumps(log, indent=4))
                print()
                logging.error(json.dumps(log))


if __name__ == "__main__":

    threading.Thread(target=receive_heartbeat).start()
    threading.Thread(target=monitor_nodes).start()
    threading.Thread(target=receive_logs).start()
