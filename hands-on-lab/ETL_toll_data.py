import pandas as pd
import tarfile
import urllib3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


dest_path = '/home/project/airflow/dags/python_etl/staging/'


def download_dataset():
    http = urllib3.PoolManager()
    resp = http.request("GET", "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz", preload_content=False)
    path = dest_path + 'tolldata.tgz'

    with open(path, 'wb') as out:
        while True:
            data = resp.read()
            if not data:
                break
            out.write(data)

    resp.release_conn()


def untar_dataset():
    with tarfile.open(dest_path + 'tolldata.tgz', 'r') as tar:
        tar.extractall(path=dest_path)


def extract_data_from_csv():
    output = dest_path + 'csv_data.csv'
    with open(dest_path + 'vehicle-data.csv', 'r') as infile:
        with open(output, 'w') as outfile:
            for line in infile:
                fields = line.split(',')
                rowid = fields[0]
                timestamp = fields[1]
                anonymized_vehicle_number = fields[2]
                vehicle_type = fields[3]
                outfile.write(rowid + "," + timestamp + "," + anonymized_vehicle_number + "," + vehicle_type)


def extract_data_from_tsv():
    output = dest_path + 'tsv_data.csv'
    with open(dest_path + 'tollplaza-data.tsv', 'r') as infile:
        with open(output, 'w') as outfile:
            for line in infile:
                fields = line.split('\t')
                number_of_axles = fields[3]
                tollplaza_id = fields[4]
                tollplaza_code = fields[5]
                outfile.write(number_of_axles + "," + tollplaza_id  + "," + tollplaza_code)


def extract_data_from_fixed_width():
    output = dest_path + 'fixed_width_data.csv'
    with open(dest_path + 'payment-data.txt', 'r') as infile:
        with open(output, 'w') as outfile:
            for line in infile:
                line_len = len(line) - 1
                vehicle_code_width = line_len - 6
                payment_code_width = vehicle_code_width - 4

                payment_code = line[payment_code_width:vehicle_code_width]
                vehicle_code = line[vehicle_code_width:line_len]

                outfile.write(payment_code + "," + vehicle_code)


def consolidate_data():
    output = dest_path + 'extracted_data.csv'
    csv_df = pd.read_csv(dest_path + 'csv_data.csv')
    tsv_df = pd.read_csv(dest_path + 'tsv_data.csv')
    width_df = pd.read_csv(dest_path + 'fixed_width_data.csv')

    merged = pd.concat([csv_df, tsv_df, width_df], axis=1)
    merged.to_csv(path_or_buf=output, index=False)


def transform_data():
    output = dest_path + 'transformed_data.csv'
    with open(dest_path + 'extracted_data.csv', 'r') as infile:
        with open(output, 'w') as outfile:
            for line in infile:
                fields = line.split(',')
                field0 = fields[0]
                field1 = fields[1]
                field2 = fields[2]
                field3 = fields[3].upper()
                field4 = fields[4]
                field5 = fields[5]
                field6 = fields[6]
                field7 = fields[7]
                field8 = fields[8]
                outfile.write(field0 + "," + field1 + "," + field2 + "," + field3 + "," + field4 + "," + field5 + "," + field6 + "," + field7 + "," + field8)

default_args = {
    'owner': 'Lisa Simpson',
    'start_date': days_ago(0),
    'email': ['lisasimpson@bleedinggums.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

etl_toll_data_dag = DAG(
    dag_id = "ETL_toll_data",
    description = 'Apache Airflow Final Assignment',
    schedule = "@daily",
    default_args = default_args,
)

first_task = PythonOperator(
    task_id='download_data',
    python_callable=download_dataset,
    dag=etl_toll_data_dag,
)

second_task = PythonOperator(
    task_id='unzip_data',
    python_callable=untar_dataset,
    dag=etl_toll_data_dag,
)

third_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=etl_toll_data_dag,
)

fourth_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=etl_toll_data_dag,
)

fifth_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=etl_toll_data_dag,
)

sixth_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=etl_toll_data_dag,
)

seventh_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=etl_toll_data_dag,
)


first_task >> second_task >> third_task >> fourth_task >> fifth_task >> sixth_task >> seventh_taskirst_task >> second_task >> third_task >> fourth_task >> fifth_task >> sixth_task >> seventh_task
