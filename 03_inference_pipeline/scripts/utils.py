
###############################################################################
# Import necessary modules
# ##############################################################################

import mlflow
import mlflow.sklearn

import mlflow.lightgbm
import pandas as pd

import sqlite3

import os
import logging

from  constants import * 
from datetime import datetime
import mlflow
import mlflow.sklearn
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.metrics import accuracy_score,classification_report,confusion_matrix
import collections
from sklearn.model_selection import train_test_split

###############################################################################
# Define the function to train the model
# ##############################################################################

def load_data(file_path_list):
    data=[]
    for eachfile in file_path_list:
        data.append(pd.read_csv(eachfile,index_col=0))
    return data


def read_input_data(DB_PATH,DB_FILE_NAME,input_table="model_input"):
    try:
        db_path = os.path.join(DB_PATH, DB_FILE_NAME)
        conn = sqlite3.connect(db_path)
        print("✅ Successfully connected to DB")
        df = pd.read_sql(f"SELECT * FROM {input_table}", conn)
        print("Input data has been extracted successfully")
        conn.close()
        return df
    except Exception as e:
        return f"❌ Error: {e}"

def save_to_db(DB_PATH,DB_FILE_NAME,input_data,tablename):
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        input_data.to_sql(name= tablename, con=conn, if_exists='replace', index=False)
        print("input_data has been saved to table " + tablename)
        conn.close()
    except Exception as e:
        return f"❌ Error: {e}"

def encode_features(DB_PATH,DB_FILE_NAME,ONE_HOT_ENCODED_FEATURES,FEATURES_TO_ENCODE):
    
    input_data=read_input_data(DB_PATH,DB_FILE_NAME)
    encoded_df=pd.DataFrame(columns=ONE_HOT_ENCODED_FEATURES)
    df_placeholder=pd.DataFrame()
    
    for f in FEATURES_TO_ENCODE:
        if f in input_data.columns:
            encoded = pd.get_dummies(input_data[f])
            encoded = encoded.add_prefix(f + '_')
            df_placeholder=pd.concat([df_placeholder,encoded],axis=1)
        else :
            print('Feature not found')
    for feature in encoded_df:
        if feature in input_data.columns:
            encoded_df[feature] = input_data[feature]
        if feature in df_placeholder.columns:
            encoded_df[feature] = df_placeholder[feature] 
    encoded_df.fillna(0, inplace=True)
    save_to_db(DB_PATH,DB_FILE_NAME,encoded_df,'features_inference')
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
        **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
    '''

###############################################################################
# Define the function to load the model from mlflow model registry
# ##############################################################################

def get_models_prediction():
   

    mlflow.set_tracking_uri(TRACKING_URI)


    model_uri = f"models:/{MODEL_NAME}/{STAGE}"
    model = mlflow.lightgbm.load_model(model_uri)
    df_feature_inf=read_input_data(DB_PATH,DB_FILE_NAME,'features_inference')
    y_pred = model.predict(df_feature_inf)
    predicted_output = pd.DataFrame(y_pred,columns=['predicted_output'])
    save_to_db(DB_PATH,DB_FILE_NAME,predicted_output,'predicted_output')
    
    '''
    This function loads the model which is in production from mlflow registry and 
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage. 

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    '''

###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################

def prediction_ratio_check():
    '''
    This function calculates the % of 1 and 0 predicted by the model and  
    and writes it to a file named 'prediction_distribution.txt'.This file 
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline 
    folder. 
    This helps us to monitor if there is any drift observed in the predictions 
    from our model at an overall level. This would determine our decision on 
    when to retrain our model.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with 
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    '''
    input_data = read_input_data(DB_PATH,DB_FILE_NAME, 'predicted_output')
    outputfile_name ='Scripts_output_prediction_distribution_'+ datetime.now().strftime("%Y%m%d%H%M%S") +'.txt'
    #input_data.to_csv(outputfile_name, header=None, index=None, sep='\t')
    
    output = input_data.groupby(['predicted_output']).size().reset_index(name='counts')
    count_0 = output[output['predicted_output'] == 0]
    count_0 = count_0['counts'][0]
    count_1 = output[output['predicted_output'] == 1]
    count_1 = count_1['counts'][1]

    result_1 = round((count_1/len(input_data.index))*100, 2)
    result_0 = round((count_0/len(input_data.index))*100, 2)
    data = {'is_churn':['0', '1'], 'percentage(%)':[result_0, result_1]}  
    result_df = pd.DataFrame(data)
    result_df.set_index(['is_churn'])
    result_df.to_csv(outputfile_name, header=None, index=None, sep='\t')

    print('Output file has been generated successfully ' + outputfile_name)
###############################################################################
# Define the function to check the columns of input features
# ##############################################################################


def input_features_check(db_path, db_file_name, one_hot_encoded_features):

    input_data = read_input_data(db_path, db_file_name, 'features_inference')
    if 'index' in input_data.columns:
        input_data = input_data.drop(columns=['index'])
    
    source_cols = input_data.columns.to_list()
    expected_cols = one_hot_encoded_features  

    # Compare and print result
    if collections.Counter(source_cols) == collections.Counter(expected_cols):
        print('✅ All model inputs are present')
    else:
        print('⚠️ Some model inputs are missing or extra')
