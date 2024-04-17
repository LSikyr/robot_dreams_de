import json
import os
import shutil

import requests
from flask import Flask, request

AUTH_TOKEN = os.environ.get("AUTH_TOKEN")

app = Flask(__name__)


def get_data(date=None):
    page_count = 1
    data = []
    while True:
        response = requests.get(
            url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
            params={
                'date': date,
                'page': page_count
            },
            headers={'Authorization': AUTH_TOKEN}
        )

        if response.status_code != 200:
            break

        data.extend(response.json())
        page_count += 1
    return data


def save_data(raw_dir=None, date=None, data=None):
    filename = f'sales_{date}.json'
    file_path = os.path.join(raw_dir, filename)

    if os.path.exists(raw_dir):
        shutil.rmtree(raw_dir)
    os.makedirs(raw_dir)

    with open(file_path, 'w') as f:
        json.dump(data, f)


@app.route('/', methods=['POST'])
def main():
    request_body = request.json

    date = request_body.get('date')
    raw_dir = request_body.get('raw_dir')

    if not date or not raw_dir:
        return {
            "message": "Missing required parameter. [date] and [raw_dir] must be set",
        }, 400

    data = get_data(date=date)

    if not data:
        return {
            "message": f'No data found for date {date}'
        }, 404

    save_data(
        raw_dir=raw_dir,
        date=date,
        data=data
    )

    return {
        "message": f'Data saved into: {raw_dir}'
    }, 201


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8081, debug=True)