# You can create more variables according to your project. The following are the basic variables that have been provided to you
DB_PATH = '/Users/arunprakash/Downloads/Assignment/01_data_pipeline/scripts/'
#DB_FILE_NAME = 'utils_output.db'
DB_FILE_NAME = 'lead_scoring_data_cleaning.db'
UNIT_TEST_DB_FILE_NAME = 'unit_test_cases.db'
DATA_DIRECTORY = '/Users/arunprakash/Downloads/Assignment/01_data_pipeline/scripts/data/'
INTERACTION_MAPPING = '/Users/arunprakash/Downloads/Assignment/01_data_pipeline/scripts/mapping/'
INDEX_COLUMNS_TRAINING = ['created_date', 'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier','referred_lead','app_complete_flag']
INDEX_COLUMNS_INFERENCE = ['created_date', 'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier','referred_lead']
NOT_FEATURES = ['syllabus_interaction', 'assistance_interaction', 'created_date', 'social_interaction', 'career_interaction', 'payment_interaction']