##############################################################################
# Import the necessary modules
##############################################################################

import os
import pytest
import sqlite3
import pandas as pd
import constants

from utils import build_dbs, load_data_into_db, map_city_tier, map_categorical_vars, interactions_mapping
from city_tier import city_tier_mapping
import significant_categorical_level

TEST_DB_FILE_NAME = "unit_test_cases.db"
TEST_DB_PATH = os.path.abspath(".")  
INTERACTION_MAPPING = "/Users/arunprakash/Downloads/Assignment/01_data_pipeline/unit_test/"

TEST_DATA_DIR = "/Users/arunprakash/Downloads/Assignment/01_data_pipeline/unit_test/"

###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def test_load_data_into_db():
    assert load_data_into_db(TEST_DB_PATH, TEST_DB_FILE_NAME, TEST_DATA_DIR, "leadscoring_test.csv","loaded_data_test_case") == "loaded_data updated."
    conn = sqlite3.connect(os.path.join(TEST_DB_PATH, TEST_DB_FILE_NAME))
    assert not pd.read_sql("SELECT * FROM loaded_data_test_case", conn).empty, "❌ Data not loaded!"
    conn.close()

###############################################################################
# Write test cases for map_city_tier() function
###############################################################################
def test_map_city_tier():
    
    assert map_city_tier(TEST_DB_PATH, TEST_DB_FILE_NAME,city_tier_mapping,"loaded_data_test_case","city_tier_mapped_test_case") == "city_tier_mapped table updated."
    conn = sqlite3.connect(os.path.join(TEST_DB_PATH, TEST_DB_FILE_NAME))
    assert not pd.read_sql("SELECT * FROM city_tier_mapped_test_case", conn).empty, "❌ Data not loaded!"
    conn.close()


###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
      
    assert map_categorical_vars(TEST_DB_PATH,TEST_DB_FILE_NAME,significant_categorical_level.list_platform,
                    significant_categorical_level.list_medium,significant_categorical_level.list_source,
                    "city_tier_mapped_test_case","categorical_variables_mapped_test_case") == "categorical_variables_mapped updated."
    conn = sqlite3.connect(os.path.join(TEST_DB_PATH, TEST_DB_FILE_NAME))
    assert not pd.read_sql("SELECT * FROM categorical_variables_mapped_test_case", conn).empty, "❌ Data not loaded!"
    conn.close()
###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
  

  #conn = sqlite3.connect(TEST_DB_PATH + TEST_DB_FILE_NAME)
  # Fetch all table names
  #query = "SELECT name FROM sqlite_master WHERE type='table';"
  #tables = pd.read_sql(query, conn)
  #conn.close()
  #print(tables)
  #print(TEST_DB_FILE_NAME) 
  assert interactions_mapping(TEST_DB_PATH,TEST_DB_FILE_NAME,INTERACTION_MAPPING,constants.INDEX_COLUMNS_TRAINING,
            constants.INDEX_COLUMNS_INFERENCE,constants.NOT_FEATURES,"categorical_variables_mapped_test_case","interactions_mapped_test_case",
                             ) == "interactions_mapped and model_input updated."
  conn = sqlite3.connect(os.path.join(TEST_DB_PATH, TEST_DB_FILE_NAME))
  assert not pd.read_sql("SELECT * FROM interactions_mapped_test_case", conn).empty, "❌ Data not loaded!"
  conn.close()
