from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'William',  # Replace with your name
    'start_date': datetime(2024, 11, 19),  
    'email': ['Williamlo@example.com'],  #
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',  # Runs daily
    catchup=False,
) as dag:

    # Task 1: Unzip the data
    unzip_data = BashOperator(
        task_id='unzip_data',
        bash_command='tar -xvf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging/',
    )

    # Task 2: Extract data from CSV
    extract_data_from_csv = BashOperator(
        task_id='extract_data_from_csv',
        bash_command="cut -d',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv",
    )

    # Task 3: Extract data from TSV
    extract_data_from_tsv = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command="cut -f5,6,7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv",
    )

    # Task 4: Extract data from fixed-width file
    extract_data_from_fixed_width = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command="cut -c59-69,70-78 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv",
    )

    # Task 5: Consolidate the data
    consolidate_data = BashOperator(
        task_id='consolidate_data',
        bash_command="paste -d',' /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv",
    )

    # Task 6: Transform the data
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command="awk 'BEGIN{FS=OFS=\",\"} {for(i=1;i<=NF;i++)$i=toupper($i)}1' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    )

    # Define the task pipeline
    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
