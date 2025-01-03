import json
import random
import threading
import time
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, request

NODE_ID = 2


heartbeat = {
    "node_id": NODE_ID,
    "message_type": "HEARTBEAT",
    "status": "UP",
    "timestamp": f"{datetime.now()}",
}

registration = {
    "node_id": NODE_ID,
    "message_type": "REGISTRATION",
    "service_name": "OrderService",
    "timestamp": f"{datetime.now()}",
}


def random_log():
    num = random.randint(1, 100)

    # INFO log
    if num >= 1 and num < 40:
        log = {
            "log_id": str(uuid.uuid4()),
            "node_id": NODE_ID,
            "log_level": "INFO",
            "message_type": "LOG",
            "message": "Order processed successfully",
            "service_name": "OrderService",
            "timestamp": time.time(),
        }

    # WARN log
    elif num >= 40 and num < 75:
        response_time = random.randint(500, 1500)
        threshold_limit = random.randint(500, 1000)
        log = {
            "log_id": str(uuid.uuid4()),
            "node_id": NODE_ID,
            "log_level": "WARN",
            "message_type": "LOG",
            "message": "Order processing delay detected",
            "service_name": "OrderService",
            "response_time_ms": response_time,
            "threshold_limit_ms": threshold_limit,
            "timestamp": time.time(),
        }

    # ERROR log
    else:
        log = {
            "log_id": str(uuid.uuid4()),
            "node_id": NODE_ID,
            "log_level": "ERROR",
            "message_type": "LOG",
            "message": "Order creation failed",
            "service_name": "OrderService",
            "error_details": {
                "error_code": str(uuid.uuid4()),
                "error_message": "Payment gateway unavailable",
            },
            "timestamp": time.time(),
        }

    return log


def send_logs():

    try:
        while True:
            log = random_log()
            response = requests.post("http://localhost:5001/orders-logs", json=log)

            if response.status_code == 200:
                print("Log Sent")
            else:
                print(f"Failed to send log: {response.status_code}")
            time.sleep(1)

    except KeyboardInterrupt:
        print()
        print("Log Generation Stopped!")


def send_hearbeat():
    while True:
        response = requests.post("http://localhost:5001/heartbeat", json=heartbeat)

        if response.status_code == 200:
            print("HEARTBEAT SENT")
        else:
            print(f"Failed to send HEARTBEAT: {response.status_code}")

        time.sleep(3)


if __name__ == "__main__":

    threading.Thread(target=send_logs).start()
    threading.Thread(target=send_hearbeat).start()
