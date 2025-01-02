import json

from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    "info_log",
    "warn_log",
    "error_log",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


for message in consumer:
    log = message.value

    if log["log_level"] == "INFO":

        pass  # store stuff

    else:

        if log["log_level"] == "WARN":
            print("WARNING LOG:")
            print(json.dumps(log, indent=4))
            print()

        elif log["log_level"] == "ERROR":
            print("ERROR LOG:")
            print(json.dumps(log, indent=4))
            print()
