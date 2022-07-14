
from glob import glob
import datetime
import hashlib
import json
import dateutil
from google.cloud import bigquery

files = glob("alerts-endpoint/**/**/**.ndjson")
project = "elastic-security-prod"
dataset_name = f"{project}.elastic_security_telemetry"
client = bigquery.Client(project)

for file in files:
    with open(file, "r") as inf:
        to_insert = []
        lines = inf.readlines()
        for line in lines:
            jdata = json.loads(line)
            timestamp = str(jdata.get("original-body",{}).get("@timestamp"))
            if not "Z" in timestamp:
                s = 1236472051807 / 1000.0
                timestamp = datetime.datetime.utcfromtimestamp(s).strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                timestamp = dateutil.parser.parse(timestamp)
            hash = hashlib.sha256(line.encode("utf-8")).hexdigest()
            to_insert.append((timestamp, hash, line))
        table = client.get_table(f"{dataset_name}.alerts-endpoint")
        ret = client.insert_rows(table, to_insert)
        print(ret)
