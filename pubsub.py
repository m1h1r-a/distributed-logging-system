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
        print(json.dumps(log, indent=4))
        print()
        # store log

    else:
        pass  # send alerts
