from datetime import datetime, timedelta, timezone
import time
from pandas import read_csv, concat, pivot_table, to_datetime

import boto3
from s3fs import S3FileSystem

# Create a boto3 session to interact with S3/AWS
session = boto3.Session()
s3 = session.client('s3', region_name='us-east-1')
rds = boto3.client('rds', region_name='us-east-1')

import os
bucket_name = os.getenv('BUCKET_NAME')

# local application/library specific imports
from airflow import DAG
from airflow.models import DagRun
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

# Instantiate the DAG
dag = DAG(
    dag_id='transform_and_upsert',
    default_args=default_args,
    # this means the DAG will run every day at the same time as start date
    schedule="@monthly"
)

def read_combines_new_files_from_s3(**kwargs):
    
    # Get the list of objects in the source bucket
    objects = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix='inputs/')['Contents']

    # this returns a dict (within a list) with the names and properties of
    # the objects in our bucket

   # Find the last successful run of the current DAG
    dag_runs = DagRun.find(dag_id='transform_and_upsert', state='success')
    if len(dag_runs) > 0:
        # Use the execution date of the last successful run
        last_dag_run_date = dag_runs[-1].execution_date
    else:
        # If this is the first run, or no successful runs, use a date far in the past
        last_dag_run_date = datetime(2000, 1, 1, tzinfo=timezone.utc)

    print('Last dag run date: ', last_dag_run_date)

    # For the objects that are more recent than the last dag run date,
    # reads them (using read_csv) and combines in them in a pandas dataframe
    dfs = []
    df_combined = None
    for obj in objects:
        print('Last modified object: ', obj['LastModified'])
        if obj['LastModified'] > last_dag_run_date:
            dfs.append(read_csv(f's3://{bucket_name}/' + obj['Key']))
    
    if len(dfs) > 0:
        df_combined = concat(dfs, axis=0)
        df_combined.to_csv(f's3://{bucket_name}/intermediate_data/df_combined.csv', index=False)
        return True
    else:
        return False

def branch_function(**kwargs):
    ti = kwargs['ti']
    # Pull the Boolean value indicating if the df was saved successfully
    df_saved_to_s3 = ti.xcom_pull(task_ids='read_combines_new_files_from_s3')

    if df_saved_to_s3:
        return 'pivoting_df'
    else:
        return 'end_task'


def pivoting_df(**kwargs):
    s3_file_path = f's3://{bucket_name}/intermediate_data/df_combined.csv'
    df = read_csv(s3_file_path)

    df = df.drop('End date', axis=1)

    # Converting the column 'Start date' to date
    df['Start date'] = df['Start date'].str[:10]

    df['Start date'] = to_datetime(
        df['Start date'],
        format='%Y-%m-%d').dt.date
    
    # Aggregate 'Duration (in minutes)' by 'Start date' and 'Project'
    df = df.groupby(['Start date', 'Project']).sum().reset_index()

    df_pivoted = pivot_table(
       data=df,
       values="Duration (in minutes)",
       index="Start date",
       columns="Project",
       fill_value= 0
    )

    # Rename columns
    df_pivoted = df_pivoted.rename(
       columns={'Learning': 'learning_minutes',
                'Work': 'work_minutes'}
       ).reset_index()

    df_pivoted.columns.name = None

    # Remove df_combined.csv from S3
    file_key = 'intermediate_data/df_combined.csv'
    s3.delete_object(Bucket=bucket_name, Key=file_key)

    df_pivoted.to_csv(f's3://{bucket_name}/intermediate_data/df_pivoted.csv', index=False)

# Define the function that performs the upsert
def upsert_df_to_rds(**kwargs):
    db_instance_identifier = 'airflow-postgres'

    s3_file_path = f's3://{bucket_name}/intermediate_data/df_pivoted.csv'
    
    # Read the DataFrame directly from the S3 CSV file
    # specifying data types so they match the destination table
    dtype_spec = {'work_minutes': 'float', 'learning_minutes': 'float'}
    df = read_csv(s3_file_path, dtype=dtype_spec, parse_dates=['Start date'])

    # Change Database PubliclyAccessible to True
    rds.modify_db_instance(
            DBInstanceIdentifier=db_instance_identifier,
            PubliclyAccessible=True,
            ApplyImmediately=True
        )
    
    # This change is not applied immediatly, so we need to wait for it to be applied
    elapsed_time = 0
    timeout = 300
    interval = 30
    is_public = False

    while elapsed_time < timeout:
        try:
            db_instances = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
            db_instance = db_instances['DBInstances'][0]
            is_public = db_instance['PubliclyAccessible']

            if is_public == True:
                print(f"RDS instance '{db_instance_identifier}' public accessibility is now {is_public}.")
                time.sleep(40)
                break
            else:
                print(f"Waiting for RDS instance '{db_instance_identifier}' to become {'publicly accessible' if True else 'private'}. Checking again in {interval} seconds.")
                time.sleep(interval)
                elapsed_time += interval
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    if not is_public:
        print(f"Timeout reached: RDS instance '{db_instance_identifier}' did not reach the desired state within {timeout} seconds.")
        # Exit task
        return

    # Get the RDS connection using PostgresHook
    rds_hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    # Get the table name and schema from the parameters
    table = kwargs['params']['table']
    schema = kwargs['params']['schema']

    # Ensure the DataFrame has columns in the correct order
    expected_columns = ['Start date', 'learning_minutes', 'work_minutes']
    df = df[expected_columns]
    
    # Create a temporary table with the same structure as the target table
    rds_hook.run(f"CREATE TEMPORARY TABLE tmp_{table} (LIKE {schema}.{table});")
    
    # Insert the dataframe into the temporary table using to_sql method
    df.to_sql(
       f"tmp_{table}",
       rds_hook.get_sqlalchemy_engine(),
       schema=schema,
       if_exists='replace',
       index=False)
    
    # Perform the upsert by merging the temporary table with the target table on the date column
    rds_hook.run(f"""
        INSERT INTO {schema}.{table}
        SELECT * FROM tmp_{table}
        ON CONFLICT (date) DO UPDATE SET
        work_minutes = EXCLUDED.work_minutes,
        learning_minutes = EXCLUDED.learning_minutes;
    """)
    
    # Drop the temporary table
    rds_hook.run(f"DROP TABLE tmp_{table};")

    # Remove df_pivoted.csv from S3
    file_key = 'intermediate_data/df_pivoted.csv'
    s3.delete_object(Bucket=bucket_name, Key=file_key)

    # Set PubliclyAccessible=False to save money
    rds.modify_db_instance(
            DBInstanceIdentifier=db_instance_identifier,
            PubliclyAccessible=False,
            ApplyImmediately=True
        )

# Define the task that performs the upsert using PythonOperator
upsert_task = PythonOperator(
    task_id='upsert_df_to_rds',
    python_callable=upsert_df_to_rds,
    params={
        'table': 'pomodoro_day_catg',
        'schema': 'public'
    },
    provide_context=True,
    dag=dag,
)

read_combines_new_files_from_s3 = PythonOperator(
        task_id='read_combines_new_files_from_s3',
        python_callable=read_combines_new_files_from_s3,
        dag=dag)

branch_task = BranchPythonOperator(
  task_id='branch_task',
  python_callable=branch_function,
  provide_context=True,
  dag=dag
)

pivoting_df = PythonOperator(
  task_id='pivoting_df',
  python_callable=pivoting_df,
  provide_context=True,
  dag=dag
)

# define the end_task as a DummyOperator
end_task = DummyOperator(
  task_id='end_task',
  dag=dag
)

# set the dependencies
read_combines_new_files_from_s3 >> branch_task
branch_task >> pivoting_df >> upsert_task
branch_task >> end_task
