import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from google.cloud import storage, bigquery

url="https://api.breakingbadquotes.xyz/v1/quotes"
quotesFile = 'quotes.json'
bucket='bigmoney-quotes'
table = "bigmoneydata.quotesDS.quotesTable"

def getAndWriteQuote(url, outputFile):
  quote=requests.get(url=url).json()[0]

  with open(outputFile, 'a') as file:
      file.write(f"{str(quote)}\n")
  return quote

def pythonToGCS(bucket, fileName):
  gcsBlob = storage.Client().get_bucket(bucket).blob(fileName)
  gcsBlob.upload_from_filename(fileName)
  return 0


def gcsToBq(gcsBucket, gcsFile,table_id):
  # Construct a BigQuery client object.
  client = bigquery.Client()
  uri = f"gs://{gcsBucket}/{gcsFile}"
  
  job_config = bigquery.LoadJobConfig(
      schema=[bigquery.SchemaField("author", "STRING"),bigquery.SchemaField("quote", "STRING"), ],
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  )
  
  load_job = client.load_table_from_uri(
      uri,table_id,location="EU",  
      job_config=job_config,
  )  
  load_job.result()  
  destination_table = client.get_table(table_id)  # Make an API request.
  print("Loaded {} rows.".format(destination_table.num_rows))
  return 0


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 23),  
    'retries': 1,
}


# Define the DAG
with DAG('write_and_copy_file',
         default_args=default_args,
         schedule_interval='@daily',  # Set to None or a cron schedule if needed
         catchup=False) as dag:

    # Task 1: Write the file using Python
    getQuoteWriteFile= PythonOperator(
        task_id='getQuoteWriteFileTask',
        python_callable=getAndWriteQuote,
        op_args=[url,quotesFile]
    )

    # Task 2: Copy the file one directory up using Bash
    #copyToGCS = BashOperator(
    #    task_id='copyToGCS',
    #    bash_command=f'gsutil cp {quotesFile} {bucket} '
    #    )
    # Task 2: Write the file using Python
    copyToGCS= PythonOperator(
        task_id='copyToGCS',
        python_callable=pythonToGCS,
        op_args=[bucket, quotesFile]
    )

    ## Task 3: Copy the file one directory up using Bash
    #format="NEWLINE_DELIMITED_JSON"
    #loadToBq = BashOperator(
    #    task_id='bq_load',
    #    bash_command=f'bq load --source_format=${format} quotes.quotesTable {bucket}/{quotesFile} schema.json'
    #    )
    loadToBq = PythonOperator(
        task_id='bq_load',
        python_callable=gcsToBq,
        op_args=[bucket, quotesFile, table]
        )

    # Set the task dependencies
    getQuoteWriteFile >> copyToGCS >> loadToBq