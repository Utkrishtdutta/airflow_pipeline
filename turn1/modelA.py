
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
	'retries': 2,
	'retry_delay': timedelta(minutes=5),
}

with DAG('smart_grid_data_analysis_pipeline', default_args=default_args, schedule_interval='0 0 * * *', catchup=False) as dag:

	def fetch_data_from_source():
		# Replace this with your actual data fetching logic
		data = pd.DataFrame({
			'timestamp': pd.date_range(start='2023-01-01', periods=100, freq='H'),
			'voltage': np.random.randn(100),
			'current': np.random.randn(100),
		})
		return data

	def clean_data(data):
		# Replace this with your data cleaning logic
		data = data.dropna()
		return data

	def perform_analysis(data):
		# Replace this with your data analysis logic
		result = data['voltage'].mean()
		print(f"Average Voltage: {result}")
		return result


	t1 = PythonOperator(
		task_id='fetch_data_from_source',
		python_callable=fetch_data_from_source,
		provide_context=True
	)

	t2 = PythonOperator(
		task_id='clean_data',
		python_callable=clean_data,
		op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="fetch_data_from_source") }}'},
		provide_context=True
	)

	t3 = PythonOperator(
		task_id='perform_analysis',
		python_callable=perform_analysis,
		op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="clean_data") }}'},
		provide_context=True
	)

t1 >> t2 >> t3
