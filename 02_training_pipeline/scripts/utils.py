
###############################################################################
# Import necessary modules
# ##############################################################################

import pandas as pd
import numpy as np
import os
import sqlite3
from sqlite3 import Error

import mlflow.lightgbm
from lightgbm import LGBMClassifier

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score,f1_score,precision_score,recall_score,classification_report,confusion_matrix

from constants import *


###############################################################################
# Define the function to encode features
# ##############################################################################
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
#######################################
def encode_features(DB_FILE_NAME,DB_PATH,ONE_HOT_ENCODED_FEATURES):

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
    # Save the encoded features in a table - features
    save_to_db(DB_PATH,DB_FILE_NAME,encoded_df,'features')
    # Save the target variable in a separate table - target
    target=input_data[["app_complete_flag"]]
    save_to_db(DB_PATH,DB_FILE_NAME,target,'target')
   
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline from the pre-requisite module for this.
    '''


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():

    X=read_input_data(DB_PATH,DB_FILE_NAME,'features')
    print(X.head())
    Y=read_input_data(DB_PATH,DB_FILE_NAME,'target')
    print(Y.head())
    X_train,X_test,y_train,y_test = train_test_split(X,Y,test_size=0.2,random_state=100)



    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run():
    # Train model
        model = LGBMClassifier(**model_config)
        model.fit(X_train, y_train)
    #Logs the trained model into mlflow model registry with name 'LightGBM'
    mlflow.lightgbm.log_model(
        model,
        artifact_path="model",
        registered_model_name="LightGBM"
    )
    mlflow.log_params(model_config)
   
    #Logs the metrics and parameters into mlflow run
    y_pred=model.predict(X_test)
    acc = accuracy_score(y_test,y_pred)
    mlflow.log_metric("test_accuracy", acc)
    precision=precision_score(y_test,y_pred)
    mlflow.log_metric("Precision", precision)
    recall=recall_score(y_test,y_pred)
    mlflow.log_metric("Recall", recall)
    #Calculate auc from the test data and log into mlflow run  
    auc= roc_auc_score(y_test,y_pred)
    print("AUC=", auc)
    mlflow.log_metric('auc',auc)
    
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        
        Logs the trained model into mlflow model registry with name 'LightGBM'
        
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''

   
