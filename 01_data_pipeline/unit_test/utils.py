##############################################################################
# Import necessary modules and files
##############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))

# Move up one level to reach 01_data_pipeline/
project_dir = os.path.abspath(os.path.join(script_dir, "../"))

# Define the correct path to notebooks/maps/
maps_dir = os.path.join(project_dir, "notebooks", "maps")

# Add notebooks/maps/ to sys.path so Python can find city_tier.py
sys.path.append(maps_dir)


sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import constants

mapping_path = os.path.join(script_dir, "mapping")  # Update with actual folder name
sys.path.append(mapping_path)

import significant_categorical_level
from city_tier import city_tier_mapping

###############################################################################
# Define the function to build database
###############################################################################

#This function checks if the db file with specified name is present in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates the db file with the given name at the given path. 
#  INPUTS DB_FILE_NAME  : Name of the database file 'utils_output.db' DB_PATH : path where the db file should exist  
#  OUTPUT The function returns the following under the given conditions:
#        1. If the file exists at the specified path
#                prints 'DB Already Exists' and returns 'DB Exists'

#        2. If the db file is not present at the specified loction
#                prints 'Creating Database' and creates the sqlite db 
#                file at the specified path with the specified name and 
#                once the db file is created prints 'New DB Created' and 
#                returns 'DB created'



def build_dbs(DB_PATH,DB_FILE_NAME):
    if os.path.isfile(DB_PATH+DB_FILE_NAME):
        print( "DB Already Exsist")
        print(os.getcwd())
        return "DB Exist"
    else:
        print ("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:            
            conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
            print("New DB Created")
        except Error as e:
            print(e)
            return "Error"
        finally:
            if conn:
                conn.close()
                return "DB Created"
             
###############################################################################
# Define function to load the csv file to the database
###############################################################################
#This function loads the data present in data directory into the db which was created previously.It also replaces any null values present in 'toal_leads_dropped' and 'referred_lead' columns with 0.
#INPUTS :DB_FILE_NAME  : Name of the database file
#        DB_PATH : path where the db file should be
#        DATA_DIRECTORY : path of the directory where 'leadscoring.csv'  file is present
# OUTPUT
#   Saves the processed dataframe in the db in a table named 'loaded_data'.  If the table with the same name already exsists then the function replaces it.

def load_data_into_db(DB_PATH,DB_FILE_NAME,DATA_DIRECTORY,filename="leadscoring.csv",tablename ="loaded_data"):
    try:
        if filename == "leadscoring.csv":
            df = pd.read_csv(f"{DATA_DIRECTORY}{filename}",index_col=0)
        else:
            df = pd.read_csv(f"{DATA_DIRECTORY}{filename}")
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        try:
            df.to_sql(name= tablename, con=conn, if_exists='replace', index=False)
        except Exception as e:
            print(f"❌ SQLite Write Error: {e}")
            conn.close()
            return "loaded_data updated."
        try:
            df_check = pd.read_sql(f"SELECT COUNT(*) FROM {tablename};", conn)
            print(f"📌 Rows in '{tablename}': {df_check.iloc[0, 0]}")
            
        except Exception as e:
            print(f"❌ Error checking data in DB: {e}")
        conn.close()
        return "loaded_data updated."

    except Exception as e:
        return f"Error in loading data into_db: {e}"
###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

#This function maps all the cities to their respective tier as per the mappings provided in the city_tier_mapping.py file. If ap articular city's tier isn't mapped(present) in the city_tier_mapping.py file then the function maps that particular city to 3.0 which represents tier-3. 

# INPUT DB_FILE_NAME  : Name of the database file, 
#        DB_PATH : path where the db file should be ,
#        city_tier_mapping : a dictionary that maps the cities to their tier

# OUTPUT Saves the processed dataframe in the db in a table named 'city_tier_mapped'. If the table with the same name already exsists then the function replaces it.
    
def map_city_tier(DB_PATH,DB_FILE_NAME,city_tier,input_table="loaded_data", output_table="city_tier_mapped"):
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        df = pd.read_sql(f"SELECT * FROM {input_table}", conn)
       
        df["city_tier"] = df["city_mapped"].map(city_tier_mapping)
        df["city_tier"] = df["city_tier"].fillna(3.0)
        df.to_sql(name=output_table, con=conn, if_exists='replace', index=False)
        
        conn.close()
        return"city_tier_mapped table updated."
    except Exception as e:
        return f"❌ Error: {e}"

###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def process_categorical_vars(df,list_platform,list_medium,list_source):
    for column in ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']:
        df_cat_freq = df[column].value_counts()
        df_cat_freq = pd.DataFrame({'column':df_cat_freq.index, 'value':df_cat_freq.values})
        df_cat_freq['perc'] = df_cat_freq['value'].cumsum()/df_cat_freq['value'].sum()

        new_df = df[~df['first_platform_c'].isin(list_platform)].copy() # get rows for levels which are not present in list_platform
        new_df['first_platform_c'] = "others" # replace the value of these levels to others
        old_df = df[df['first_platform_c'].isin(list_platform)] # get rows for levels which are present in list_platform
        df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

        new_df = df[~df['first_utm_medium_c'].isin(list_medium)].copy() # get rows for levels which are not present in list_medium
        new_df['first_utm_medium_c'] = "others" # replace the value of these levels to others
        old_df = df[df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are present in list_medium
        df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

    # all the levels below 90 percentage are assgined to a single level called others
        new_df = df[~df['first_utm_source_c'].isin(list_source)].copy() # get rows for levels which are not present in list_source
        new_df['first_utm_source_c'] = "others" # replace the value of these levels to others
        old_df = df[df['first_utm_source_c'].isin(list_source)] # get rows for levels which are present in list_source
        df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
        
        df['total_leads_droppped'].fillna(0, inplace=True)
        df['referred_lead'].fillna(0, inplace=True)
    return df


def map_categorical_vars(DB_PATH,DB_FILE_NAME,list_platform,list_medium,
                         list_source,input_table="city_tier_mapped", output_table="categorical_variables_mapped"):
#    This function maps all the insignificant variables present in 'first_platform_c'
#    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
#    should be stored in a python file in the 'significant_categorical_level.py' 
#    so that it can be imported as a variable in utils file.
    

#    INPUTS
#        DB_FILE_NAME  : Name of the database file
#        DB_PATH : path where the db file should be present
#        list_platform : list of all the significant platform.
#        list_medium : list of all the significat medium
#        list_source : list of all the significant source

#        **NOTE : list_platform, list_medium & list_source are all constants and
#                 must be stored in 'significant_categorical_level.py'
#                 file. The significant levels are calculated by taking top 90
#                 percentils of all the levels. 
  

#    OUTPUT
#        Saves the processed dataframe in the db in a table named
#        'categorical_variables_mapped'. If the table with the same name already 
#        exsists then the function replaces it.

    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        df = pd.read_sql(f"SELECT * FROM {input_table}", conn)
        df=process_categorical_vars(df,list_platform,list_medium,list_source)
        df.to_sql(name=output_table, con=conn, if_exists='replace', index=False)
        conn.close()
        return "categorical_variables_mapped updated."
    
    except Exception as e:
        print(f"❌ Error: {e}")
##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
#  This function maps the interaction columns into 4 unique interaction columns.These mappings are present in 'interaction_mapping.csv' file.
#  Inputs:DB_FILE_NAME, DB_PATH, INTERACTION_MAPPING, INDEX_COLUMNS_TRAINING, INDEX_COLUMNS_INFERENCE, and NOT_FEATURES
#  Outputs:The function saves the processed DataFrame in the database as `'interactions_mapped'`, replacing it if it exists, and also drops unnecessary features before saving the final dataset as `'model_input'`.

def interactions_mapping(DB_PATH,DB_FILE_NAME,INTERACTION_MAPPING,INDEX_COLUMNS_TRAINING,INDEX_COLUMNS_INFERENCE,NOT_FEATURES,
                          input_table="categorical_variables_mapped",output_table="interactions_mapped"):
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        cursor=conn.cursor()
        df = pd.read_sql(f"SELECT * FROM {input_table}", conn)
       
        df = df.drop(['city_mapped'], axis = 1)
        df = df.drop_duplicates()
        df_event_mapping = pd.read_csv(f"{INTERACTION_MAPPING}interaction_mapping.csv", index_col=[0])
        result = cursor.execute(f"SELECT 1 FROM pragma_table_info('{input_table}') WHERE name='app_complete_flag'").fetchone()
        df_pivot= process_interactions_mapping(result,df,df_event_mapping,constants.INDEX_COLUMNS_TRAINING,constants.INDEX_COLUMNS_INFERENCE)
        
        df_pivot.to_sql(name=output_table, con=conn, if_exists='replace', index=False)  
        count = pd.read_sql(f"SELECT COUNT(*) FROM {output_table}", conn).iloc[0, 0]
        print(f"✅ interactions_mapped. Total Rows: {count}")
        
        df_model_input = df_pivot.drop(NOT_FEATURES, axis=1)
        df_model_input.to_sql(name='model_input', con=conn,if_exists='replace',index=False)
        count = pd.read_sql("SELECT COUNT(*) FROM model_input", conn).iloc[0, 0]
        print(df_model_input.columns)
        print(f"✅ model_input updated. Total Rows: {count}")

        conn.close()
        return "interactions_mapped and model_input updated." 
    except Exception as e:
        return f"❌ Error: {e}"
#####################
def process_interactions_mapping(result, df, df_event_mapping,INDEX_COLUMNS_TRAINING,INDEX_COLUMNS_INFERENCE):
       
    if result:
        df_unpivot = pd.melt(df, id_vars= INDEX_COLUMNS_TRAINING, var_name='interaction_type', value_name='interaction_value')
        df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
        df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
        df = df.drop(['interaction_type'], axis=1)
        df_pivot = df.pivot_table(values='interaction_value', index=constants.INDEX_COLUMNS_TRAINING, columns='interaction_mapping', aggfunc='sum')
        df_pivot = df_pivot.reset_index()
       
    else :
        df_unpivot = pd.melt(df, id_vars= INDEX_COLUMNS_INFERENCE,var_name='interaction_type', value_name='interaction_value')
        df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
        df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
        df = df.drop(['interaction_type'], axis=1)
        df_pivot = df.pivot_table(values='interaction_value', index=constants.INDEX_COLUMNS_INFERENCE, columns='interaction_mapping', aggfunc='sum')
        df_pivot = df_pivot.reset_index() 
    return df_pivot