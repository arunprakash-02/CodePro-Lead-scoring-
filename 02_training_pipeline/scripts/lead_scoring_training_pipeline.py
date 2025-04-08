##############################################################################
# Import necessary modules
# #############################################################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import sys
from constants import * 
import os
sys.path.append(os.path.dirname(__file__))

from utils import encode_features
###############################################################################
# Define default arguments and DAG
# ##############################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,8,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_training_dag = DAG(
                dag_id = 'Lead_scoring_training_pipeline',
                default_args = default_args,
                description = 'Training pipeline for Lead Scoring System',
                schedule_interval = '@monthly',
                catchup = False
)
###############################################################################
# Create a task for encode_features() function with task_id 'encoding_categorical_variables'
# ##############################################################################
op_encode_features = PythonOperator(
                    task_id='encoding_categorical_variables',
                    python_callable=encode_features,
                    op_kwargs={
                    'DB_PATH': constants.DB_PATH,
                    'DB_NAME': constants.DB_FILE_NAME,
                   'ONE_HOT_ENCODED_FEATURES':constants.ONE_HOT_ENCODED_FEATURES
                     },
    dag=ML_training_dag
)
###############################################################################
# Create a task for get_trained_model() function with task_id 'training_model'
# ##############################################################################
op_get_trained_model= PythonOperator(
                    task_id='training_model',
                    python_callable=encode_features,
                    op_kwargs={
                    'DB_PATH': constants.DB_PATH,
                    'DB_NAME': constants.DB_FILE_NAME,
                   'ONE_HOT_ENCODED_FEATURES':constants.ONE_HOT_ENCODED_FEATURES
                     },
    dag=ML_training_dag
)

###############################################################################
# Define relations between tasks
# ##############################################################################

encode_features.set_downstream(get_trained_model)