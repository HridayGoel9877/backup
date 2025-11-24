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


# Create an external table in BigQuery 
def create_external_table(project_id, dataset_id, packets, dates, **kwargs):
    import time
    client = bigquery.Client(project=project_id)
    # fs = gcsfs.GCSFileSystem(project=project_id)
    # dates = [item.split('/')[-1] for item in fs.glob("gs://gcp_casestudy_bucket/Data/*")] 
    # dates = ["2024-08-01", "2024-08-02"]
    ext_execution = {}
    # print("Entering loop")
    for packet_name in packets:
        
        table_id = f"{packet_name}_external"
        gcs_uris = [f"gs://gcp_casestudy_bucket/Data/{date}/{packet_name}/*.parquet" for date in dates]
       
        # Define the SQL query to create or replace the external table
        external_table_creation=f"""
        CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.{table_id}`
        OPTIONS (
            format = 'PARQUET',
            uris = {gcs_uris}
        )
        """
        
        # Execute the query
        start_time = time.time()
        query_job = client.query(external_table_creation)
        
        # Wait for the query to finish
        query_job.result()  # Block until the query completes
        end_time = time.time()
        # executionTime = end_time - start_time
        ext_tbl_name=f"{packet_name}_external"
        # Count of records in table
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
    # ti = kwargs['ti']
    # ti.xcom_push(key='ext_execution', value=ext_execution)
    return ext_execution
    

def store_external_table_data(project_id, dataset_id, table_id,packet,col_df, **kwargs):
    import time
    client = bigquery.Client(project=project_id)
    dtca_case = ''
    if packet=="DTCA":
        dtca_case="EXCEPT(eDateTime)"
    # print(new_col_df)
    # old_name = new_col_df[table_id]["Parquet File"]
    # new_name = new_col_df[table_id]["BigQuery"]
    new_col_df = col_df[packet]
    new_col_dict = dict(zip(new_col_df['Parquet File'], new_col_df['BigQuery']))
    select_columns = []
    bq_execution={}
    for col, new_col in new_col_dict.items():
        select_columns.append(f"{col} AS {new_col}")
    # Define the SQL query to store data from the external table to a BigQuery table
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
    
    # Execute the query to load the data into a BigQuery table
    start_time = time.time()
    query_job = client.query(sql_query)
    
    # Wait for the query to finish
    query_job.result()  # Block until the query completes
    end_time = time.time()
    # time = end_time - start_time
    bq_tbl_name=f"{packet}_bigquery"
    # Count of records in table
    query = f"""
    SELECT COUNT(*) AS record_count
    FROM `{project_id}.{dataset_id}.{bq_tbl_name}`
    """
    query_job = client.query(query)
    result = query_job.result()
    # bq_execution[packet_name]["rec_count"] = result.to_dataframe().iloc[0, 0]
    # bq_execution[packet_name]["time"] =  end_time - start_time
    # bq_execution[packet_name]["ext_tbl_name"] = f"{packet_name}_bigquery"
    print(f"Data from external table `{project_id}.{dataset_id}.{table_id}` has been loaded into BigQuery table `{project_id}.{dataset_id}.{packet}_bigquery`")
    exec_stat = {
        "bq_rec_count" : result.to_dataframe().iloc[0, 0],
        "load_time" : end_time - start_time,
        "bq_tbl_name" : f"{packet}_bigquery"
    }
    return exec_stat



# Build and run the dynamic SQL query to calculate metrics
def calculate_metrics_query(project_id, dataset_id, table_id):
    from google.cloud import bigquery, storage
    import pandas as pd
    client = bigquery.Client(project=project_id)
    
    # Get columns dynamically from the INFORMATION_SCHEMA
    query = f"""
    SELECT column_name
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_id}'
    """
    
    query_job = client.query(query)
    metadata=['eDateTime','eDate','packetName','deviceName','errorFlag','sdpArrivalTime','stage1ProcessingTime',
             'stage2ProcessingTime','stage3ProcessingTime','dwIngestionTime','createdDate','updatedDate','schemaVersion']
    columns = [row['column_name'] for row in query_job if row['column_name'] not in metadata]


    # Construct the aggregation query for each column
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
    
    # Combine all individual queries into one big query with UNION ALL
    full_query = " UNION ALL ".join(aggregation_queries)

    # Run the combined query
    query_job = client.query(full_query)
    results = query_job.result()  # Wait for the query to finish

    # Convert results to a Pandas DataFrame
    df = pd.DataFrame([dict(row.items()) for row in results])
    return df


# Write the metrics to Excel and upload to GCS
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
    # exec_output_path = "gs://gcp_casestudy_bucket/validation/execution_stats.xlsx"
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
    # return exec_stats
    temp='/tmp/execution_stats.xlsx'
    with pd.ExcelWriter(temp, engine='openpyxl', mode='w') as writer:
        exec_stats_df.to_excel(writer, sheet_name='execution_stats', index=False)
    # return 0
    client = storage.Client()
    bucket_name, blob_name = exec_output_path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(temp)
    # return exec_stats_df


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
    
    # 1. Create/replace error table
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

    # 2. Load error mapping from GCS
    client = storage.Client()
    bucket_name, blob_path = error_mapping_gcs_path.replace("gs://", "").split("/", 1)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    data = blob.download_as_bytes() 
    # data = storage_client.bucket(bucket_name).blob(blob_path).download_as_bytes()
    df = pd.read_excel(BytesIO(data), engine="openpyxl", sheet_name=None)
    # print(df['DTCA'])
    # return
    # column_type_mapping = dict(zip(df['BigQuery_Bronze'], df['Data Type']))
    
    # 3. Define excluded columns
    excluded_columns = {
        "packetName", "deviceName", "errorFlag", "sdpArrivalTime",
        "stage1ProcessingTime", "stage2ProcessingTime", "stage3ProcessingTime",
        "dwIngestionTime", "createdDate", "updatedDate", "schemaVersion", "eDate"
    }

    # 4. Process each packet
    for packet in packets:
        print(f"\nStarting error checks for packet: {packet}")
        column_type_mapping = dict(zip(df[packet]['BigQuery_Bronze'], df[packet]['Data Type']))
        # print(packet, column_type_mapping)
        # continue
        # Initialize list for error check expressions
        error_checks = []
        
        # Process each column in the mapping
        for column_name, data_type in column_type_mapping.items():
            # Skip excluded columns
            if column_name in excluded_columns:
                continue
                
            # Generate the error check expression
            error_expr = f"""
            CASE WHEN {column_name} IS NOT NULL AND SAFE_CAST({column_name} AS {data_type}) IS NULL 
                THEN STRUCT('{column_name}' AS key, 'Failed to cast to {data_type}' AS value) 
                ELSE NULL END
            """
            error_checks.append(error_expr)
        
        # Build the complete query
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
        # print(query)
        # print(f"Checking {len(error_checks)} columns in {packet}")
        query_job=client.query(query)
        query_job.result()

def combine_and_upload_excel_from_gcs(gcs_uri1, gcs_uri2, output_gcs_uri):
    local_filepath = '/tmp/combined_metrics.xlsx'

    # Initialize the GCS client
    client = storage.Client()

    # Parse the GCS URI for both input files
    bucket_name1, blob_name1 = gcs_uri1.replace("gs://", "").split("/", 1)
    bucket_name2, blob_name2 = gcs_uri2.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name1)
    
    # Download the files from GCS
    blob1 = bucket.blob(blob_name1)
    blob2 = bucket.blob(blob_name2)
    
    excel_data1 = blob1.download_as_bytes()
    excel_data2 = blob2.download_as_bytes()

    # Load the Excel data into pandas DataFrames (multiple sheets)
    df1 = pd.read_excel(excel_data1, sheet_name=None)
    df2 = pd.read_excel(excel_data2, sheet_name=None)

    # Initialize a dictionary to store the combined DataFrames
    combined_sheets = {}

    # Compare the sheets from both files and concatenate if the sheet names match
    for sheet_name in df1.keys():
        pack = sheet_name.split('_')[0]
        sheet2_name = f"{pack}_bigquery"
        if sheet2_name in df2:
            # Concatenate the data from both sheets and sort by column name
            combined_df = pd.concat([df1[sheet_name], df2[sheet2_name]], ignore_index=True)
            combined_df = combined_df.sort_values(by=list(combined_df.columns))
            # Sort columns alphabetically
            combined_sheets[pack] = combined_df

    # Call the function to write the combined data to Excel and upload to GCS
    write_to_excel_and_upload_fn(combined_sheets, local_filepath, output_gcs_uri)

    print(f"Combined Excel file uploaded to {output_gcs_uri}")

def generate_bronze_layer(project_id, dataset_id, packets, mapping_gcs_path):
    import pandas as pd
    from google.cloud import storage, bigquery
    from io import BytesIO
    
    # Initialize clients
    client = bigquery.Client(project=project_id)
    storage_client = storage.Client()
    
    # Load type mapping from GCS
    bucket_name, blob_path = error_mapping_gcs_path.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    data = blob.download_as_bytes() 
    df = pd.read_excel(BytesIO(data), engine="openpyxl", sheet_name=None)

    # Define metadata columns to include
    metadata_columns = [
        "packetName", "deviceName", "errorFlag", "sdpArrivalTime",
        "stage1ProcessingTime", "stage2ProcessingTime", "stage3ProcessingTime",
        "dwIngestionTime", "createdDate", "updatedDate", "schemaVersion", "eDate",
         "eDateTime"
    ]

    # Process each packet
    for packet in packets:
        print(f"\nProcessing packet: {packet}")
        
        # Get column to data type mapping from Excel
        column_type_mapping = dict(zip(df[packet]['BigQuery_Bronze'], df[packet]['Data Type']))
        
        # Initialize empty lists
        metadata_selects = []
        typecast_selects = []
        
        # Process metadata columns (include as-is)
        for col in metadata_columns:
            metadata_selects.append(f"{col}")
        
        # Process other columns (with typecasting)
        for column_name, data_type in column_type_mapping.items():
            # if column_name not in metadata_columns:
            typecast_selects.append(f"CAST({column_name} AS {data_type}) AS {column_name}")
        
        # Combine both lists for final SELECT
        # all_selects = metadata_selects + typecast_selects
        
        # Build the final query
        bronze_query = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{packet}_BRONZE` AS
        SELECT
            {",".join(metadata_selects)},
            {",".join(typecast_selects)}
        FROM `{project_id}.{dataset_id}.{packet}_bigquery` 
        """
        
        # Execute the query
        # print(f"Creating typed table for {packet}...")
        # query_job = client.query(query)
        # query_job.result()
        # print(f"Successfully created {packet}_BRONZE with:")

        final_query = f"""
        CREATE OR REPLACE PROCEDURE `mystical-slate-448109-u0.data_explore`.{packet}_BRONZE_STOREPROC()
        BEGIN
        {bronze_query};
        END;
        """
        query_job = client.query(final_query)
        query_job.result()
    
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
pq_path =Variable.get("pq_path")  # GCS path for the Excel file
bq_path=Variable.get("bq_path")
output_gcs_uri=Variable.get("output_gcs_uri")
pq_tables = json.loads(Variable.get("pq_tables"))
bq_tables = json.loads(Variable.get("bq_tables"))
error_mapping_gcs_path = Variable.get("error_mapping_gcs_path")
exec_output_path= Variable.get("exec_output_path")


# bq_tables = ['CANBS4_bigquery','CAN3BS6_bigquery','CAN2BS6_bigquery','DTCA_bigquery']
# error_mapping_gcs_path = "gs://gcp_casestudy_bucket/mapping/errorcapture.xlsx"
# exec_output_path= "gs://asia-south1-case-study-envi-e3745256-bucket/data/execution_stats.xlsx"

    # client = storage.Client()
    # bucket_name, blob_path = error_mapping_gcs_path.replace("gs://", "").split("/", 1)
    # bucket = client.bucket(bucket_name)
    # blob = bucket.blob(blob_path)
    # data = blob.download_as_bytes() 
    # data = storage_client.bucket(bucket_name).blob(blob_path).download_as_bytes()
    # df = pd.read_excel(BytesIO(data), engine="openpyxl", sheet_name=None)

import gcsfs
fs = gcsfs.GCSFileSystem(project=project_id)
path = "gs://gcp_casestudy_bucket/mapping/raw-bigquery-satge4.xlsx"
with fs.open(path, mode="rb") as f:
    new_col_df = pd.read_excel(f, engine="openpyxl", sheet_name=None, usecols="B:C")
dates = [item.split('/')[-1] for item in fs.glob("gs://gcp_casestudy_bucket/Data/*")] 





with DAG(
    dag_id='final_dag',
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

    # def create_stats_update(packets, **context):
    #     exec_stats = {}
    #     for packet in packets:
    #         temp_stats = context['ti'].xcom_pull(task_ids=f'store_external_table_data_{packet}')
    #         if temp_stats and packet in exec_stats:
    #             exec_stats[packet].update(temp_stats)
        # execution_stats(packets, exec_stats,exec_output_path)
    def create_stats_update(packets, **context):
        exec_stats = {}
        for packet in packets:
            temp_stats = context['ti'].xcom_pull(task_ids=f'store_external_table_data_{packet}')
            stats=temp_stats[packet]
            # if temp_stats and packet in exec_stats:
            #     exec_stats[packet].update(temp_stats)
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
    create_external_table_op>>bigquery_table_data_ops>>create_stats_task>>calculate_metrics_op>>combine_excel_op>>generate_error_checks_op>>send_email>>bronze_layer_task
    # create_external_table_op
    # create_external_table_op>>bigquery_table_data_ops>>calculate_metrics_op>>combine_excel_op>>generate_error_checks_op