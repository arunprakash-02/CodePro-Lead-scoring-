##############################################################################
# Import necessary modules
# #############################################################################

from utils import *
from data_validation_checks import *
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import constants
import significant_categorical_level
import sys
import os

sys.path.append("/Users/arunprakash/airflow/dags")
sys.path.append("/Users/arunprakash/airflow/dags/mapping")
from mapping.city_tier_mapping import city_tier_mapping

###############################################################################
# Define default arguments and DAG
###############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_data_cleaning_dag = DAG(
                dag_id = 'Lead_Scoring_Data_Engineering_Pipeline',
                default_args = default_args,
                description = 'DAG to run data pipeline for lead scoring',
                schedule_interval = '@daily',
                catchup = False
)

###############################################################################
# Create a task for build_dbs() function with task_id 'building_db'
###############################################################################
# First task - building_db (This one is correct)
op_building_db = PythonOperator(
    task_id='building_db',
    python_callable=build_dbs,
    op_kwargs={'DB_PATH': constants.DB_PATH, 'DB_NAME': constants.DB_FILE_NAME},
    dag=ML_data_cleaning_dag
)

# Second task - checking_raw_data_schema (This one is correct)
op_checking_raw_data_schema = PythonOperator(
    task_id='checking_raw_data_schema',
    python_callable=raw_data_schema_check,
    op_kwargs={'data_directory': constants.DATA_DIRECTORY},
    dag=ML_data_cleaning_dag
)

# Third task - loading_data (Corrected version)
op_loading_data = PythonOperator(
    task_id='loading_data',
    python_callable=load_data_into_db,
    op_kwargs={
        'DB_PATH': constants.DB_PATH,
        'DB_NAME': constants.DB_FILE_NAME,
        'DATA_DIRECTORY': constants.DATA_DIRECTORY
    },
    dag=ML_data_cleaning_dag
)
###############################################################################
# Create a task for map_city_tier() function with task_id 'mapping_city_tier'
###############################################################################

op_mapping_city_tier = PythonOperator(
    task_id='mapping_city_tier',
    python_callable=map_city_tier,
    op_kwargs={
        'DB_PATH': constants.DB_PATH,
        'DB_NAME': constants.DB_FILE_NAME,
        'city_tier': city_tier_mapping
    },
    dag=ML_data_cleaning_dag
)
###############################################################################
# Create a task for map_categorical_vars() function with task_id 'mapping_categorical_vars'
###############################################################################
op_map_categorical_vars = PythonOperator(
    task_id='mapping_categorical_vars',
    python_callable=map_categorical_vars,
    op_kwargs={
        'DB_PATH': constants.DB_PATH,
        'DB_NAME': constants.DB_FILE_NAME,
        'list_platform': significant_categorical_level.list_platform,
        'list_medium': significant_categorical_level.list_medium,
        'list_source': significant_categorical_level.list_source
    },
    dag=ML_data_cleaning_dag
)
###############################################################################
# Create a task for interactions_mapping() function with task_id 'mapping_interactions'
###############################################################################
op_interactions_mapping = PythonOperator(
    task_id='mapping_interactions',
    python_callable=interactions_mapping,
    op_kwargs={
        'DB_PATH': constants.DB_PATH,
        'DB_NAME': constants.DB_FILE_NAME,
        'INTERACTION_MAPPING':constants.INTERACTION_MAPPING,
        'INDEX_COLUMNS_TRAINING':constants.INDEX_COLUMNS_TRAINING,
        'INDEX_COLUMNS_INFERENCE':constants.INDEX_COLUMNS_INFERENCE,
        'NOT_FEATURES':constants.NOT_FEATURES
    },
    dag=ML_data_cleaning_dag
)
###############################################################################
# Create a task for model_input_schema_check() function with task_id 'checking_model_inputs_schema'
###############################################################################    
op_model_input_schema_check = PythonOperator(
    task_id='checking_model_inputs_schema',
    python_callable=model_input_schema_check,
    op_kwargs={
        'DB_PATH': constants.DB_PATH,
        'DB_NAME': constants.DB_FILE_NAME,
    },
    dag=ML_data_cleaning_dag
) 
###############################################################################
# Define the relation between the tasks
###############################################################################
op_building_db.set_downstream(op_checking_raw_data_schema)
op_checking_raw_data_schema.set_downstream(op_loading_data)
op_loading_data.set_downstream(op_mapping_city_tier)
op_mapping_city_tier.set_downstream(op_map_categorical_vars)
op_map_categorical_vars.set_downstream(op_interactions_mapping)
op_interactions_mapping.set_downstream(op_model_input_schema_check)
