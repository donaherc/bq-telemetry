from google.cloud import bigquery

project = "elastic-security-prod"
dataset_name = f"{project}.elastic_security_telemetry"

client = bigquery.Client(project)

dataset = bigquery.Dataset(dataset_name)
try:
    dataset = client.create_dataset(dataset, timeout=30)
except:
    pass

channels = [
'alerts-detections',
'alerts-endpoint',
'alerts-timeline',
'endpoint-metadata',
'security-insights-v1',
'security-lists-v2',
'security-lists',
]


for channel in channels:
    try:
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("doc_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("doc", "JSON", mode="REQUIRED")
        ]
        table= bigquery.Table(f"{dataset_name}.{channel}", schema=schema)
        client.create_table(table)
    except:
        pass