"""
Shogo
BigQuery - affise uploading
"""

from google.cloud import bigquery

import os
import json
from operation_bigquery import Bigquery_to_Pandas
import pandas as pd
import glob
import logging
logging.basicConfig(filename=r'C:\Users\sh_uchida\Desktop\tech_centre/logger.log', level=logging.DEBUG)

PROJECT_ID = 'product-perry'
SERVICE_ACCOUNT = 'perry-big-query@product-perry.iam.gserviceaccount.com'
#PRIVATE_KEY_PATH = r'C:\Users\sh_uchida\Desktop\DB\test-bigquery-test-d41824d6661a.p12'
#json_key = r'C:\Users\sh_uchida\Desktop\DB\Test-002d037599fe.json'

#with open(PRIVATE_KEY_PATH, 'r', encoding=) as f:
#    private_key = f.read()
    #private_key = private_key.decode()

#jsonpass = r"C:\Users\sh_uchida\Desktop\DB\Test-daabca035d95.json"
jsonpass = r"C:\Users\sh_uchida\Desktop\DB\product-perry-6c51c5bdbeb8.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = jsonpass
client = bigquery.Client(project=PROJECT_ID)
                    #json_key_file=json_key,
                    #service_account= SERVICE_ACCOUNT,
                    #readonly=False)



# ---------------------------- appsflyer --------------------------------#

def upload_files_installevent(af_list):
    for af_name in af_list:
        report_types = ["Installs", "Events"]
        for report_type in report_types:
            folder = r"C:\Users\sh_uchida\Desktop\DB\files\Appsflyer\\" + af_name + r"\\" + report_type
            file_path = max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime)
            print(file_path)
            dataset_id = "Appsflyer"
            table_id = report_type
            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
            job.result()  # Waits for the job to complete.
            logging.debug("----------------" + af_name + report_type + "bq upload all done----------------")



def upload_files_retention(af_list):
    for af_name in af_list:
        folder = r"C:\Users\sh_uchida\Desktop\DB\files\Appsflyer\\" + af_name + r"\Retention\\"
        file_path = max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime)
        print(file_path)
        dataset_id = "Appsflyer"
        table_id = "Retention"
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  # Waits for the job to complete.


# ---------------------------- adjust --------------------------------#
def upload_files_deliverable(ad_list):
    for ad_name in ad_list:
        report_types = ["Deliverable-adgroup", "Deliverable-adgroup-event", "Deliverable-campaign", "Deliverable-campaign-event"]
        for report_type in report_types:
            folder = r"C:\Users\sh_uchida\Desktop\DB\files\Adjust\\" + ad_name + r"\\" + report_type
            file_path = max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime)
            print(file_path)
            dataset_id = "Adjust"
            table_id = report_type
            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
            job.result()  # Waits for the job to complete.
            logging.debug("----------------" + ad_name + report_type + " all done----------------")


def upload_files_cohort(ad_list):
    for ad_name in ad_list:
        folder = r"C:\Users\sh_uchida\Desktop\DB\files\Adjust\\" + ad_name + r"\Cohort\\"
        file_path = max(glob.glob(os.path.join(folder, "*.csv")), key=os.path.getctime)
        print(file_path)
        dataset_id = "Adjust"
        table_id = "Cohort"
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  # Waits for the job to complete.
        logging.debug("----------------" + ad_name + " cohort done----------------")







####################### one file upload ############################
def one_upload_files(folder, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    with open(folder, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()  # Waits for the job to complete.






####################### creating new table ############################


def create_new_table(dataset_name, table_name, schema_path):

    DATASET = dataset_name
    TABLE_NAME = table_name
    SCHEMA_PATH = schema_path

    with open(SCHEMA_PATH, 'r') as f:
        table_schema = json.load(f)
    if not client.check_dataset(DATASET):
        client.create_dataset(DATASET)
    if not client.check_table(DATASET, TABLE_NAME):
        client.create_table(DATASET, TABLE_NAME, table_schema)




def change_dateformat(location, columns):
    print(location)
    report = pd.read_csv(location, encoding='utf-8-sig')
    for column in columns:
        report[column] = report[column].strftime("%Y-%m-%d")



