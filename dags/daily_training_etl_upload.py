from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pymysql
import os
from dotenv import load_dotenv
from dateutil import parser

# Load environment variables
load_dotenv("/opt/airflow/.env")

default_args = {
    'owner': 'venkatesh',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='daily_training_etl_upload',
    default_args=default_args,
    description='ETL DAG to load training data into MySQL',
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['training', 'ETL'],
) as dag:

    def extract():
        try:
            folder_path = "/opt/airflow/training_files"
            filenames = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
            if not filenames:
                raise FileNotFoundError("No CSV files found in training_files folder.")
            
            file_path = os.path.join(folder_path, filenames[0])
            df = pd.read_csv(file_path)
            print(f"Extracted {len(df)} rows from {file_path}")

            df.to_pickle('/tmp/extracted_training.pkl')
        except Exception as e:
            print(f"Error in Extract step: {e}")
            raise

    def transform():
        try:
            df = pd.read_pickle('/tmp/extracted_training.pkl')
            df.dropna(inplace=True)

            # Clean and standardize column names
            df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

            # Robust date parsing
            def parse_date(val):
                try:
                    return parser.parse(str(val)).date().strftime('%Y-%m-%d')
                except Exception:
                    return None

            df['training_date'] = df['training_date'].apply(parse_date)

            # Drop rows where date parsing failed
            before = len(df)
            df = df.dropna(subset=['training_date'])
            after = len(df)
            print(f"Transformed data. Dropped {before - after} rows with invalid dates. Remaining: {after}")

            df.to_pickle('/tmp/transformed_training.pkl')
        except Exception as e:
            print(f"Error in Transform step: {e}")
            raise

    def load():
        try:
            df = pd.read_pickle('/tmp/transformed_training.pkl')
            print(f"Loading {len(df)} rows into MySQL...")

            host = os.getenv("MYSQL_HOST")
            port = int(os.getenv("MYSQL_PORT", 3306))
            user = os.getenv("MYSQL_USER")
            password = os.getenv("MYSQL_PASSWORD")
            database = os.getenv("MYSQL_DATABASE")

            for var in [host, user, password, database]:
                if not var:
                    raise ValueError("One or more MySQL environment variables are missing.")

            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )

            cursor = conn.cursor()
            insert_query = """
                INSERT INTO training_employee_records
                (employee_id, employee_name, department, gender, training_date, training_category,
                 course, training_mode, no_of_training_session, training_hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row.values))

            conn.commit()
            cursor.close()
            conn.close()
            print("Data successfully loaded into MySQL.")

        except Exception as e:
            print(f"Error in Load step: {e}")
            raise

    extract_task = PythonOperator(task_id='extract_csv', python_callable=extract)
    transform_task = PythonOperator(task_id='transform_data', python_callable=transform)
    load_task = PythonOperator(task_id='load_into_mysql', python_callable=load)

    extract_task >> transform_task >> load_task
