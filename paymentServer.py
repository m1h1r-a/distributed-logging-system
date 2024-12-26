import json

from flask import Flask, request

app = Flask(__name__)


@app.route("/payment-logs", methods=["POST"])
def receive_logs():
    log = request.json
    # print(f"Received log: {json.dumps(log,indent = 4)}")
    print("Log Received")
    # Process and forward the log to the Pub-Sub Model
    return "Log received", 200


@app.route("/heartbeat", methods=["POST"])
def receive_heartbeat():
    heartbeat = request.json
    print(
        "--------------------------------RECEIVED HEARTBEAT--------------------------------"
    )

    return "Hearbeat received", 200


if __name__ == "__main__":
    app.run(debug=False, port=5002)
