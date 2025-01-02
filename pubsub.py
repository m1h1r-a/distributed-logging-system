import json
import threading

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


def receive_heartbeat():
    for message in heartbeat:
        heartbeat_message = message.value
        print(f"HEARTBEAT: {heartbeat_message}")


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
    threading.Thread(target=receive_logs).start()
