"""
Pipeline Module
This module defines a pipeline for loading data, preprocessing it, training models,
and saving artifacts.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from src.project_pipeline import eda, load_config
from src.project_pipeline import data_loader, model_training, save_artifacts, aws_utils

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration and environment variables
CONFIG_PATH = os.getenv('CONFIG_PATH', 'config/default.yaml')
config = load_config.load_config(Path(CONFIG_PATH))
load_dotenv()
aws_access_key = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
aws_region = os.getenv('aws_region')

# Define file paths
artifacts = Path('artifacts')
CF_MODEL_FILE = artifacts / 'Collaborative_Filtering' / 'best_cf.pkl'
CBF_MODEL_FILE = artifacts / 'Content_Based_Filtering' / 'best_cbf.pkl'
DATA_USER_SPLIT = artifacts / 'Data' / 'user_split.pkl'
DATA_BEFORE_TRAIN_PATH = artifacts / 'Data' / 'final_df.pkl'
TRAIN_DATA_PATH = artifacts / 'Data' / 'train_data.pkl'
TEST_DATA_PATH = artifacts / 'Data' / 'test_data.pkl'

logger.info('Reading data...')
df = data_loader.read_data(config['data_loader']['path'])

logger.info('Preprocessing data...')
df_processed = eda.data_preprocess(df)
df_user_split = pd.concat([eda.split_users(row)
                           for _, row in df_processed.iterrows()], ignore_index=True)

logger.info('Extracting first and last category...')
df_user_split[['First_category', 'Last_category']] = df_user_split['category'].apply(lambda x:
                                                        pd.Series(eda.extract_first_last(x)))
df_user_split.drop('category', axis=1, inplace=True)

logger.info('Performing one-hot encoding...')
df_final = eda.one_hot_encoding(df_user_split)

logger.info('Splitting data into train and test sets...')

logger.info('Splitting data into train and test sets...')
train_test_data = model_training.train_test_data(df_final, config['train_test_config']['test_size'],
                                     config['train_test_config']['random_state'],
                                     config['train_test_config']['training_cols'])

logger.info('Training Collaborative Filtering model...')
best_collaborative_filtering = model_training.collaborative_filtering(
    train_test_data[0],
    config['model_building'][0]['CF'][0]['model']['params']['n_factors'],
    config['model_building'][0]['CF'][0]['model']['params']['lr_all'],
    config['model_building'][0]['CF'][0]['model']['params']['reg_all']
)

logger.info('Training Content Based Filtering model...')
content_based_filtering = model_training.content_base_filtering(
                            config['model_building'][1]['CBF'][0]['model']['numeric_params'],
                            config['model_building'][1]['CBF'][0]['model']['text_params'],
                            train_test_data[1])


logger.info('Saving models and data...')
save_artifacts.save_model(content_based_filtering, CBF_MODEL_FILE)
save_artifacts.save_model(best_collaborative_filtering, CF_MODEL_FILE)
save_artifacts.save_data(df_user_split, DATA_USER_SPLIT)
save_artifacts.save_data(df_final, DATA_BEFORE_TRAIN_PATH)
save_artifacts.save_data(train_test_data[1], TRAIN_DATA_PATH)
save_artifacts.save_data(train_test_data[2], TEST_DATA_PATH)

logger.info('Uploading artifacts to AWS S3...')
aws_utils.upload_artifacts(aws_access_key,
                           aws_secret_access_key,
                           aws_region, artifacts,
                           config['aws'])

logger.info('Uploaded artifacts!')
