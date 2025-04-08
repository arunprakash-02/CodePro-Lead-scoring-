"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
import constants
from constants import * 
from schema import * 
import numpy as np
import sqlite3

###############################################################################
# Define function to validate raw data's schema
############################################################################### 

def raw_data_schema_check(DATA_DIRECTORY,filename="leadscoring.csv"):
    try:
        if filename == "leadscoring.csv":
            df = pd.read_csv(f"{DATA_DIRECTORY}{filename}",index_col=0)
        else:
            df = pd.read_csv(f"{DATA_DIRECTORY}{filename}")
        if sorted(raw_data_schema)== sorted(df.columns):
            print('Raw datas schema is in line with the schema present in schema.py')
        else :
            print('Raw datas schema is NOT in line with the schema present in schema.py')
    except Exception as e:
        print (f"Error in raw data schema check: {e}")
    
'''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
 
   
'''
###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check(DB_FILE_NAME,DB_PATH):
    
    conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    cursor = conn.cursor()
    
    # Read table schema
    df = pd.read_sql("SELECT * FROM model_input LIMIT 1", conn)  # Fetching only 1 row for efficiency
    conn.close()

    db_columns = set(df.columns)  # Columns in the database table
    schema_columns = set(model_input_schema)  # Expected schema

    if db_columns == schema_columns:
        print("Models input schema is in line with the schema present in schema.py")
    else:
        print("Models input schema is NOT in line with the schema present in schema.py")

        missing_columns = schema_columns - db_columns  # Columns in schema but not in DB
        extra_columns = db_columns - schema_columns  # Columns in DB but not in schema

        if missing_columns:
            print("Missing columns in DB:", missing_columns)
        if extra_columns:
            print("Extra columns in DB:", extra_columns)
   
  
'''     
This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

 
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
'''