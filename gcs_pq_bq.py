import pandas as pd
from google.cloud import bigquery, storage
import os
import gcsfs
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
import json


def create_external_table(project_id, dataset_id, packets, dates, **kwargs):
    import time
    client = bigquery.Client(project=project_id)
    
    ext_execution = {}
    
    for packet_name in packets:
        
        table_id = f"{packet_name}_external"
        gcs_uris = [f"gs://gcp_casestudy_bucket/Data/{date}/{packet_name}/*.parquet" for date in dates]
       
        
        external_table_creation=f"""
        CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.{table_id}`
        OPTIONS (
            format = 'PARQUET',
            uris = {gcs_uris}
        )
        """
        
        
        start_time = time.time()
        query_job = client.query(external_table_creation)
        
        
        query_job.result()  
        end_time = time.time()
        
        ext_tbl_name=f"{packet_name}_external"
        
        query = f"""
        SELECT COUNT(*) AS record_count
        FROM `{project_id}.{dataset_id}.{ext_tbl_name}`
        """
        query_job = client.query(query)
        result = query_job.result()
        stats = {}
        stats['rec_count'] = result.to_dataframe().iloc[0, 0]
        stats['ext_time'] =  end_time - start_time
        stats['ext_tbl_name'] = f"{packet_name}_external"
        ext_execution[packet_name] = stats
        print(f"Created or replaced external tables {packet_name}")
    
    return ext_execution
    

def store_external_table_data(project_id, dataset_id, table_id,packet,col_df, **kwargs):
    import time
    client = bigquery.Client(project=project_id)
    dtca_case = ''
    if packet=="DTCA":
        dtca_case="EXCEPT(eDateTime)"
    
    new_col_df = col_df[packet]
    new_col_dict = dict(zip(new_col_df['Parquet File'], new_col_df['BigQuery']))
    select_columns = []
    bq_execution={}
    for col, new_col in new_col_dict.items():
        select_columns.append(f"{col} AS {new_col}")
    
    sql_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{packet}_bigquery` AS
    SELECT {', '.join(select_columns)},
    CAST(TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(utc AS INT64)+946684800), INTERVAL 330 MINUTE) AS DATETIME) AS eDateTime,
    DATE(TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(utc AS INT64)+946684800), INTERVAL 330 MINUTE)) AS eDate,
    '{packet}' AS packetName,
    NULL AS deviceName,
    false AS errorFlag,
    CAST(NULL AS DATETIME) AS sdpArrivalTime,
    CAST(NULL AS DATETIME) AS stage1ProcessingTime, CAST(NULL AS DATETIME) AS stage2ProcessingTime, 
    CAST(NULL AS DATETIME) AS stage3ProcessingTime,
    CAST(NULL AS DATETIME) AS dwIngestionTime,
    CAST(NULL AS DATETIME) AS createdDate, 
    CAST(NULL AS DATETIME) AS updatedDate,
    '0.1' AS schemaVersion
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    
    
    start_time = time.time()
    query_job = client.query(sql_query)
    
    
    query_job.result()  
    end_time = time.time()
   
    bq_tbl_name=f"{packet}_bigquery"
    
    query = f"""
    SELECT COUNT(*) AS record_count
    FROM `{project_id}.{dataset_id}.{bq_tbl_name}`
    """
    query_job = client.query(query)
    result = query_job.result()
    
    print(f"Data from external table `{project_id}.{dataset_id}.{table_id}` has been loaded into BigQuery table `{project_id}.{dataset_id}.{packet}_bigquery`")
    exec_stat = {
        "bq_rec_count" : result.to_dataframe().iloc[0, 0],
        "load_time" : end_time - start_time,
        "bq_tbl_name" : f"{packet}_bigquery"
    }
    return exec_stat




def calculate_metrics_query(project_id, dataset_id, table_id):
    from google.cloud import bigquery, storage
    import pandas as pd
    client = bigquery.Client(project=project_id)
    
    
    query = f"""
    SELECT column_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_id}'
    """
    
    query_job = client.query(query)
    metadata=['eDateTime','eDate','packetName','deviceName','errorFlag','sdpArrivalTime','stage1ProcessingTime',
             'stage2ProcessingTime','stage3ProcessingTime','dwIngestionTime','createdDate','updatedDate','schemaVersion']
    columns = [row['column_name'] for row in query_job if row['column_name'] not in metadata]


    
    aggregation_queries = []
    
    for column in columns:
        aggregation_queries.append(f"""
        SELECT 
            '{column}' AS column_name,
            MIN(SAFE_CAST({column} AS FLOAT64)) AS min_value,
            MAX(SAFE_CAST({column} AS FLOAT64)) AS max_value,
            AVG(SAFE_CAST({column} AS FLOAT64)) AS avg_value,
            SUM(SAFE_CAST({column} AS FLOAT64)) AS sum_value,
            COUNT(DISTINCT {column}) AS distinct_count,
            COUNTIF({column} IS NULL) AS null_count,
            COUNTIF({column} IS NOT NULL) AS not_null_count,
            COUNTIF(SAFE_CAST({column} AS FLOAT64) = 0.0) AS zero_count
        FROM 
            `{project_id}.{dataset_id}.{table_id}`
        """)
    
    
    full_query = " UNION ALL ".join(aggregation_queries)

    
    query_job = client.query(full_query)
    results = query_job.result()  

    
    df = pd.DataFrame([dict(row.items()) for row in results])
    return df



def write_to_excel_and_upload_fn(dfs, filepath,path):
    import pandas as pd
    from google.cloud import storage 
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        for table_name, df in dfs.items():
            df.to_excel(writer, sheet_name=table_name, index=False)
   
    client = storage.Client()
    bucket_name, blob_name = path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(filepath)

def execution_stats(packets, exec_stats,exec_output_path):
    import pandas as pd
    from google.cloud import storage
   
    details = []
    for packet, stats in exec_stats.items():
        details.append([f"{packet}", ""])
        details.append(["Total Time", f"{float(stats['total time'])} seconds"])
        details.append(["External Table Time", f"{stats['ext_time']}"])
        details.append(["External Table Name", f"{stats['ext_tbl_name']}"])
        details.append(["External Table Count", f"{stats['rec_count']}"])
        details.append(["BQ Load Time", f"{stats['load_time']}"])
        details.append(["BQ Table Name", f"{stats['bq_tbl_name']}"])
        details.append(["BQ Table Count", f"{stats['bq_rec_count']}"])
    exec_stats_df = pd.DataFrame(details)
    
    temp='/tmp/execution_stats.xlsx'
    with pd.ExcelWriter(temp, engine='openpyxl', mode='w') as writer:
        exec_stats_df.to_excel(writer, sheet_name='execution_stats', index=False)
    
    client = storage.Client()
    bucket_name, blob_name = exec_output_path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(temp)
    

def gen_metrics(project_id, dataset_id, pqtables,bqtables,pq_path, bq_path ):
    dfs1 = {}
    dfs2 = {}
    filepath1= '/tmp/metrics1.xlsx'
    for table_id in pqtables:
        dfs1[table_id]= calculate_metrics_query(project_id, dataset_id, table_id)
    write_to_excel_and_upload_fn(dfs1, filepath1,pq_path)
    filepath2= '/tmp/metrics2.xlsx'
    for table_id in bqtables:
        dfs2[table_id]= calculate_metrics_query(project_id, dataset_id, table_id)
    write_to_excel_and_upload_fn(dfs2, filepath2,bq_path)

def generate_error_checks(project_id, dataset_id, packets, error_mapping_gcs_path):
    import pandas as pd
    from google.cloud import storage, bigquery
    from io import BytesIO
    client = bigquery.Client(project=project_id)
    storage_client = storage.Client()
    
    
    sql=(f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.error_capture2` (
        processedTimestamp TIMESTAMP,
        processedDate DATE,
        packetName STRING,
        deviceName INT64,
        sdpArrivalTime DATETIME,
        stage1ProcessingTime DATETIME,
        stage2ProcessingTime DATETIME,
        stage3ProcessingTime DATETIME,
        dwIngestionTime DATETIME,
        createdDate DATETIME,
        updatedDate DATETIME,
        schemaVersion STRING,
        errorType STRING,
        deviceId STRING,
        eDateTime DATETIME,
        hardErrorDescription JSON,
        rawRecord JSON
    )
    """)
    query_job = client.query(sql)
    query_job.result()
    print("Error capture table created/updated")

    
    client = storage.Client()
    bucket_name, blob_path = error_mapping_gcs_path.replace("gs://", "").split("/", 1)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    data = blob.download_as_bytes() 
    
    df = pd.read_excel(BytesIO(data), engine="openpyxl", sheet_name=None)
    
    
    
    excluded_columns = {
        "packetName", "deviceName", "errorFlag", "sdpArrivalTime",
        "stage1ProcessingTime", "stage2ProcessingTime", "stage3ProcessingTime",
        "dwIngestionTime", "createdDate", "updatedDate", "schemaVersion", "eDate"
    }

    
    for packet in packets:
        print(f"\nStarting error checks for packet: {packet}")
        column_type_mapping = dict(zip(df[packet]['BigQuery_Bronze'], df[packet]['Data Type']))
        
        error_checks = []
        
        
        for column_name, data_type in column_type_mapping.items():
            
            if column_name in excluded_columns:
                continue
                
            
            error_expr = f"""
            CASE WHEN {column_name} IS NOT NULL AND SAFE_CAST({column_name} AS {data_type}) IS NULL 
                THEN STRUCT('{column_name}' AS key, 'Failed to cast to {data_type}' AS value) 
                ELSE NULL END
            """
            error_checks.append(error_expr)
        
        
        client = bigquery.Client(project=project_id)
        query = f"""
        INSERT INTO `{project_id}.{dataset_id}.error_capture2`
        SELECT * FROM
        (SELECT
            CURRENT_TIMESTAMP() AS processedTimestamp,
            CURRENT_DATE() AS processedDate,
            '{packet}' AS packetName,
            deviceName,
            sdpArrivalTime,
            stage1ProcessingTime,
            stage2ProcessingTime,
            stage3ProcessingTime,
            dwIngestionTime,
            createdDate, 
            updatedDate,
            schemaVersion,
            'hard' AS errorType,
            deviceId, 
            eDateTime,
            (
                SELECT IF(ARRAY_AGG(key) IS NOT NULL, JSON_OBJECT(ARRAY_AGG(key), ARRAY_AGG(value)), NULL)
                FROM UNNEST([{','.join(error_checks)}]) 
                WHERE key IS NOT NULL
            ) AS hardErrorDescription,
            TO_JSON(t) AS rawRecord
        FROM `{project_id}.{dataset_id}.{packet}_bigquery` t
        )
        WHERE hardErrorDescription is NOT NULL
        """
        
        query_job=client.query(query)
        query_job.result()

def combine_and_upload_excel_from_gcs(gcs_uri1, gcs_uri2, output_gcs_uri):
    local_filepath = '/tmp/combined_metrics.xlsx'

    
    client = storage.Client()

    
    bucket_name1, blob_name1 = gcs_uri1.replace("gs://", "").split("/", 1)
    bucket_name2, blob_name2 = gcs_uri2.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name1)
    
    
    blob1 = bucket.blob(blob_name1)
    blob2 = bucket.blob(blob_name2)
    
    excel_data1 = blob1.download_as_bytes()
    excel_data2 = blob2.download_as_bytes()

    
    df1 = pd.read_excel(excel_data1, sheet_name=None)
    df2 = pd.read_excel(excel_data2, sheet_name=None)

    
    combined_sheets = {}

    
    for sheet_name in df1.keys():
        pack = sheet_name.split('_')[0]
        sheet2_name = f"{pack}_bigquery"
        if sheet2_name in df2:
            
            combined_df = pd.concat([df1[sheet_name], df2[sheet2_name]], ignore_index=True)
            combined_df = combined_df.sort_values(by=list(combined_df.columns))
            
            combined_sheets[pack] = combined_df

    
    write_to_excel_and_upload_fn(combined_sheets, local_filepath, output_gcs_uri)

    print(f"Combined Excel file uploaded to {output_gcs_uri}")

def generate_bronze_layer(project_id, dataset_id, packets, mapping_gcs_path):
    
    from google.cloud import bigquery
    
    
    
    client = bigquery.Client(project=project_id)
    

    
    for packet in packets:
        
    
        call_procedure = f"""
        CALL `mystical-slate-448109-u0.data_explore`.{packet}_BRONZE_STOREPROC();
        """
        query_job = client.query(call_procedure)
        query_job.result()
    
# if __name__ == '__main__':
# packets = ["CANBS4","CAN2BS6","CAN3BS6","DTCA"]
packets = json.loads(Variable.get("packets"))
project_id = Variable.get("project_id")
dataset_id = Variable.get("dataset_id")
pq_path =Variable.get("pq_path")  
bq_path=Variable.get("bq_path")
output_gcs_uri=Variable.get("output_gcs_uri")
pq_tables = json.loads(Variable.get("pq_tables"))
bq_tables = json.loads(Variable.get("bq_tables"))
error_mapping_gcs_path = Variable.get("error_mapping_gcs_path")
exec_output_path= Variable.get("exec_output_path")




import gcsfs
fs = gcsfs.GCSFileSystem(project=project_id)
path = "gs://gcp_casestudy_bucket/mapping/raw-bigquery-satge4.xlsx"
with fs.open(path, mode="rb") as f:
    new_col_df = pd.read_excel(f, engine="openpyxl", sheet_name=None, usecols="B:C")
dates = [item.split('/')[-1] for item in fs.glob("gs://gcp_casestudy_bucket/Data/*")] 





with DAG(
    dag_id='casestudy_gcs_bq',
    start_date=datetime(2025, 3, 19),
    schedule_interval=None,
    catchup=False
) as dag:
    create_external_table_op = PythonOperator(
        task_id='create_external_table',
        python_callable=create_external_table,
        op_args=[project_id, dataset_id, packets, dates],
        provide_context=True
    )

    bigquery_table_data_ops = []
    for packet in packets:
        def store_external_table_data_update(packet, **context):
            exec_stats = context['ti'].xcom_pull(task_ids='create_external_table')
            temp_stat = store_external_table_data(project_id, dataset_id, f"{packet}_external", packet,new_col_df)
            if packet in exec_stats:
                exec_stats[packet].update(temp_stat)
                exec_stats[packet]['total time'] = exec_stats[packet]['ext_time'] + exec_stats[packet]['load_time']
            return exec_stats

        store_in_bigquery_task = PythonOperator(
            task_id=f'store_external_table_data_{packet}',
            python_callable=store_external_table_data_update,
            op_args=[packet],
            provide_context=True
        )
        bigquery_table_data_ops.append(store_in_bigquery_task)

    def create_stats_update(packets, **context):
        exec_stats = {}
        for packet in packets:
            temp_stats = context['ti'].xcom_pull(task_ids=f'store_external_table_data_{packet}')
            stats=temp_stats[packet]
            
            exec_stats[packet]=stats
        execution_stats(packets, exec_stats,exec_output_path)
    create_stats_task = PythonOperator(
        task_id='create_stats',
        python_callable=create_stats_update,
        op_args=[packets],
        provide_context=True
    )

    calculate_metrics_op = PythonOperator(
        task_id='calculate_metrics',
        python_callable=gen_metrics,
        op_args=[project_id, dataset_id, pq_tables, bq_tables, pq_path, bq_path],
    )

    generate_error_checks_op = PythonOperator(
        task_id='generate_error_checks',
        python_callable=generate_error_checks,
        op_args=[project_id, dataset_id, packets, error_mapping_gcs_path],
    )

    combine_excel_op = PythonOperator(
        task_id='combine_excel',
        python_callable=combine_and_upload_excel_from_gcs,
        op_args=[pq_path, bq_path, output_gcs_uri],
    )
    send_email= EmailOperator(
        task_id= 'send_email',
        conn_id='connect_smtp',
        to='hriday.goel@ust.com',
        cc='sanjose.n@ust.com',
        subject='GCP Case Study Stage4 Deliverables',
        html_content='Please find attached the stats report and the validation report for all the packets and for all the dates',
        files=['/home/airflow/gcs/data/execution_stats.xlsx','/home/airflow/gcs/data/outputmetrics_final.xlsx'],
        dag=dag
    )

    bronze_layer_task=[]
    for packet in packets:
        generate_bronze_layer_op = PythonOperator(
            task_id=f"generate_bronze_layer_{packet}",
            python_callable=generate_bronze_layer,
            op_args=[project_id, dataset_id, packets, error_mapping_gcs_path]
        )
        bronze_layer_task.append(generate_bronze_layer_op)
    create_external_table_op>>bigquery_table_data_ops>>create_stats_task>>calculate_metrics_op>>combine_excel_op>>generate_error_checks_op>>bronze_layer_task>>send_email
