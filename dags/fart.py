from google.cloud import storage
from google.cloud import bigquery
from breaking_bad_quotes import getAndWriteQuote
from breaking_bad_quotes import pythonToGCS
from breaking_bad_quotes import gcsToBq
import os

print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

url="https://api.breakingbadquotes.xyz/v1/quotes"
quotesFile = 'quotes.json'
bucket='bigmoney-quotes'
table_id = "bigmoneydata.quotesDS.quotesTable"

getAndWriteQuote(url,quotesFile)

pythonToGCS(bucket,quotesFile)

gcsToBq(bucket, quotesFile,table_id )


# # Construct a BigQuery client object.
# client = bigquery.Client()
# uri = "gs://bigmoney-quotes/quotes.json"
# table_id = "bigmoneydata.quotesDS.quotesTable"
# 
# job_config = bigquery.LoadJobConfig(
#     schema=[
#         bigquery.SchemaField("author", "STRING"),
#         bigquery.SchemaField("quote", "STRING"),
#     ],
#     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
# )
# 
# load_job = client.load_table_from_uri(
#     uri,table_id,location="EU",  # Must match the destination dataset location.
#     job_config=job_config,
# )  
# load_job.result()  
# destination_table = client.get_table(table_id)  # Make an API request.
# print("Loaded {} rows.".format(destination_table.num_rows))






