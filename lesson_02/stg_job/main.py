import json
import os
import shutil

import fastavro
from flask import Flask, request

AUTH_TOKEN = os.environ.get("AUTH_TOKEN")

app = Flask(__name__)


def save_data_to_avro(stg_dir=None, filename=None, data=None):
    schema = {
        "type": "record",
        "name": "Purchase",
        "fields": [
            {"name": "client", "type": "string"},
            {"name": "purchase_date", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "int"}
        ]
    }

    file_path = os.path.join(stg_dir, filename)

    if os.path.exists(stg_dir):
        shutil.rmtree(stg_dir)
    os.makedirs(stg_dir)

    with open(file_path, "wb") as avro_file:
        fastavro.writer(avro_file, schema, data)



@app.route('/', methods=['POST'])
def main():
    request_body = request.json

    raw_dir = request_body.get('raw_dir')
    stg_dir = request_body.get('stg_dir')

    if not raw_dir or not stg_dir:
        return {
            "message": "Missing required parameter. [raw_dir] and [stg_dir] must be set",
        }, 400

    if not os.path.exists(raw_dir):
        return {"message": f'Raw file not found'}, 404

    json_file_name = os.listdir(raw_dir)[0]
    json_file_path = os.path.join(raw_dir, json_file_name)

    with open(json_file_path, 'r') as file:
        data = json.load(file)

    avro_file_name = json_file_name.replace('json', 'avro')
    save_data_to_avro(
        stg_dir=stg_dir,
        filename=avro_file_name,
        data=data
    )

    return {"message": f'Data saved into: {stg_dir}'}, 201


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8082, debug=True)