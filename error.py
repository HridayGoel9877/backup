from google.cloud import bigquery, storage
import gcsfs
from google.oauth2 import service_account
project_id="mystical-slate-448109-u0"
cred=service_account.Credentials.from_service_account_file(r"C:/Users/285905/Desktop/Gcp Case Study/stage-1/mystical-slate-448109-u0-ccf4edb860fd.json")
dataset_id="data_explore"
client=bigquery.Client(credentials=cred,project=project_id)
query=f"""CREATE OR REPLACE TABLE {project_id}.{dataset_id}.error_capture_table (
    processedTimestamp TIMESTAMP,
    processedDate DATE,
    packetName STRING,
    deviceName STRING,
    sdpArrivalTime TIMESTAMP,
    stage1ProcessingTime float64,
    stage2ProcessingTime float64,
    stage3ProcessingTime float64,
    dwIngestionTime TIMESTAMP,
    createdDate TIMESTAMP,
    updatedDate TIMESTAMP,
    schemaVersion STRING,
    errorType STRING,
    deviceId STRING,
    eDateTime TIMESTAMP,
    hardErrorDescription STRING,
    softErrorDescription STRING,
    rawRecord STRING
)"""
query_job=client.query(query)
query_job.result()

