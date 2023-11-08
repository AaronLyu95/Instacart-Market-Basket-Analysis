from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery_storage
import time

def bq_connector():
    key_path = "C:/Users/HEN1/Projects/Instacart_Market_Basket_Analysis/keys/plucky-mile-327121-255163f80b63.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    bqclient = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
    return bqclient, bqstorageclient

def bq_full_table_df(bqclient, bqstorageclient, table_name):
    sql_query = f"SELECT * FROM instacart.{table_name}"
    query_job = bqclient.query(sql_query)
    time.sleep(30)
    count =0
    while query_job.state !='DONE':
        print("NOT DONE")
        if query_job.state =='PENDING':
            print(f"job from {table_name} is pending")
            break
        if query_job.state =='RUNNING':
            print(f"job from {table_name} is running")
            print(query_job.result())
            time.sleep(60)
            query_job.reload()
            time.sleep(10)
            count += 1
            if count>3:
                break
        else:
            print("may meet an error")
            break
    if query_job.state == 'DONE':
        print(f"successfully finished getting data from {table_name} table")
        df = query_job.to_dataframe(bqstorage_client=bqstorageclient,
                                    progress_bar_type='tqdm_notebook',)
        print("successfully transferred to df")
        time.sleep(3)
    else:
        print("error")

    return df