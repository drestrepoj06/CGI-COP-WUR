from flask import Flask, jsonify, send_from_directory
import redis
import json
import os

app = Flask(__name__, static_folder="frontend-dist", static_url_path='/')

TILE38_HOST = 'tile38'
TILE38_PORT = 9851
tile38 = redis.Redis(host=TILE38_HOST, port=TILE38_PORT, decode_responses=True)

def query_tile38(collection):
    try:
        response = tile38.execute_command("SCAN", collection)
        if response:
            count, objects = response
            results = []
            for obj in objects:
                object_id = obj[0]
                coordinates_json = json.loads(obj[1])
                fields_json = json.loads(obj[2][1])
                coords = coordinates_json["coordinates"]
                results.append({
                    "id": object_id,
                    "lat": coords[1],
                    "lon": coords[0],
                    "z": coords[2] if len(coords) > 2 else None,
                    **fields_json
                })
            return results
        else:
            return []
    except Exception as e:
        print(f"Error querying {collection}: {e}")
        return []

@app.route('/api/trains')
def get_trains():
    return jsonify(query_tile38("train"))

@app.route('/api/ambulances')
def get_ambulances():
    return jsonify(query_tile38("ambulance"))

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_vue(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)