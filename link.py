from google.cloud import bigquery
from google.oauth2 import service_account
cred=service_account.Credentials.from_service_account_file(r"C:/Users/285905/Desktop/Gcp Case Study/stage-1/mystical-slate-448109-u0-ccf4edb860fd.json")
project_id="mystical-slate-448109-u0"
client = bigquery.Client(credentials=cred,project=project_id)

# Performed a query.
QUERY = (
    'SELECT distinct count(deviceId) FROM `mystical-slate-448109-u0.data_explore.CAN2BS6` LIMIT 1000')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    print(row[0])