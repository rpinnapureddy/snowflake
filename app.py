from flask import Flask, request, jsonify
import snowflake.connector
import os
import json
from waitress import serve

app = Flask(__name__)


# Snowflake connection settings
SNOWFLAKE_CONFIG = {
    'user': 'PRINGBLOM',
    'password': 'SnowFlake1234567!',
    'account': 'JZTLPWK-VT83857',
    'warehouse': 'COMPUTE_WH',
    'database': 'HUBKOR_SF_DB',
    'schema': 'RESTAPI_SCHEMA'
}

def insert_json_to_snowflake(id_value, json_payload,createdate,topic):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cs = conn.cursor()

    try:
        cs.execute(
            "INSERT INTO STAGING_TABLE (POST_ID, PAYLOAD,CREATEDATE,TOPIC) VALUES (%s, %s,%s,%s)",
            (id_value, json_payload,createdate,topic)
        )
    finally:
        cs.close()
        conn.close()

@app.route("/hello")
def home():
    return "hello"

@app.route('/ingest', methods=['POST'])
def ingest():
    content = request.json
    id_value = content.get("POST_ID")
    data = str(content.get("PAYLOAD"))
    print(data)
    createdate = content.get("CREATEDATE")
    topic=content.get("TOPIC")
    print('hello')

    if not id_value or not data:
        return jsonify({"error": "Missing id or data"}), 400

    insert_json_to_snowflake(id_value, data,createdate,topic)

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True)
    #serve(app,host='127.0.0.1',port=50100,threads=1)
    
