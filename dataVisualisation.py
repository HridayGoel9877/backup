from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
cred=service_account.Credentials.from_service_account_file(r"C:/Users/285905/Desktop/Gcp Case Study/stage-1/mystical-slate-448109-u0-ccf4edb860fd.json")
project_id="mystical-slate-448109-u0"
client = bigquery.Client(credentials=cred,project=project_id)


QUERY = (
    '''SELECT year, COUNT(*) AS total_crimes
 FROM `bigquery-public-data.chicago_crime.crime`
GROUP BY year
ORDER BY year;''')
query_job = client.query(QUERY)  
rows = query_job.result()  

data = rows.to_dataframe()
print(data)

sns.barplot(x='year',y='total_crimes',data=data)
plt.show()