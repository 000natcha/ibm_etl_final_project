from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta

input_data = '/home/project/airflow/dags/finalassignment/tolldata.tgz'
target_directory = '/home/project/airflow/dags/finalassignment/'

csv_input_directory = '/home/project/airflow/dags/finalassignment/vehicle-data.csv'
csv_output_directory = '/home/project/airflow/dags/finalassignment/csv_data.csv'

tsv_input_directory = '/home/project/airflow/dags/finalassignment/tollplaza-data.tsv'
tsv_output_directory = '/home/project/airflow/dags/finalassignment/tsv_data.csv'

txt_input_directory = '/home/project/airflow/dags/finalassignment/payment-data.txt'
txt_output_directory = '/home/project/airflow/dags/finalassignment/fixed_width_data.csv'

output_data = '/home/project/airflow/dags/finalassignment/extracted_data.csv'

transformed_directory = '/home/project/airflow/dags/finalassignment/transformed_data.csv'

default_args = {
    'owner': 'Dummy',
    'start_date': days_ago(0),
    'email': ['dummy@hotmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='ETL toll DAG',
    schedule_interval=timedelta(days=1)
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xzf {input_data} -C {target_directory}',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f'cut -d "," -f 1,2,3,4 {csv_input_directory} > {csv_output_directory}',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f'cut -f 5,6,7 {tsv_input_directory} | tr "\t" "," > {tsv_output_directory}',
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f'cut -c 59-68 {txt_input_directory} | tr " " "," > {txt_output_directory}',
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=f'paste =d "," {csv_output_directory} {tsv_output_directory} {txt_output_directory} > {output_data}',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'tr "[a-z]" "[A-Z]" > {transformed_directory}',
    dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

