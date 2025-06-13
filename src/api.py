from flask import Flask, jsonify
import redis
from flask_cors import CORS



app = Flask(__name__)
tile38 = redis.Redis(host="tile38", port=9851, decode_responses=True)

CORS(app)


@app.route("/api/train")
def get_train():
    response = tile38.execute_command("SCAN", "train")
    return jsonify(response)

@app.route("/api/ambulance")
def get_ambulance():
    response = tile38.execute_command("SCAN", "ambulance")
    return jsonify(response)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
