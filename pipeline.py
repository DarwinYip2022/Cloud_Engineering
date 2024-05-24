from pathlib import Path
import os
from dotenv import load_dotenv
import pandas as pd
import src.project_pipeline.load_config as lc
import src.project_pipeline.data_loader as dl
import src.project_pipeline.eda as eda
import src.project_pipeline.model_training as mt
import src.project_pipeline.save_artifacts as sa
import src.project_pipeline.aws_utils as aws

CONFIG_PATH = os.getenv("CONFIG_PATH", "config/default.yaml")
config = lc.load_config(Path(CONFIG_PATH))

load_dotenv()
aws_access_key = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
aws_region = os.getenv("aws_region")

MODEL_CONFIG = config["model_building"]
TRAIN_TEST_SPLIT = config["train_test_config"]

artifacts = Path() / "artifacts"

CF_MODEL_FILE = str(artifacts / "Collaborative_Filtering" / "best_cf.pkl")
CBF_MODEL_FILE = str(artifacts / "Content_Based_Filtering" / "best_cbf.pkl")
ORIGINAL_DATA_PATH = str(artifacts / "Data" / "amazon.csv")
DATA_BEFORE_TRAIN_PATH = str(artifacts / "Data" / "final_df.pkl")
TRAIN_DATA_PATH = str(artifacts / "Data" / "train_data.pkl")
TEST_DATA_PATH = str(artifacts / "Data" / "test_data.pkl")
DATA_USER_SPLIT = str(artifacts / "Data" / "user_split.pkl")


file_path = config['data_loader']['path']

df = dl.read_data(file_path)

df_processed = eda.data_preprocess(df)
df_user_split = pd.concat([eda.split_users(row) for _, row in df_processed.iterrows()], \
                          ignore_index =True)

# Apply the function to create new columns
df_user_split[['First_category', 'Last_category']] = \
df_user_split['category'].apply(lambda x: pd.Series(eda.extract_first_last(x)))
df_user_split.drop('category',axis=1,inplace=True)

df_final = eda.one_hot_encoding(df_user_split)

train_test_data = mt.train_test_data(df_final,
            TRAIN_TEST_SPLIT["test_size"],
            TRAIN_TEST_SPLIT["random_state"],
            TRAIN_TEST_SPLIT["training_cols"])

# Collaborative Filtering
CF = MODEL_CONFIG[0]['CF']
best_collaborative_filtering = mt.collaborative_filtering(
            train_test_data[0],    # The suprise Dataset training
            CF[1]["params"]["n_factors"],
            CF[1]["params"]["lr_all"],
            CF[1]['params']["reg_all"])

# Content Based Filtering
CBF = MODEL_CONFIG[1]["CBF"]
content_based_filtering = mt.content_base_filtering(
            CBF[0]["numeric_params"],
            CBF[0]["text_params"],
            train_test_data[1]
)

# save all artifacts to disk
sa.save_model(content_based_filtering, Path(CBF_MODEL_FILE))
sa.save_model(best_collaborative_filtering, Path(CF_MODEL_FILE))
sa.save_data(df_user_split, Path(DATA_USER_SPLIT))      # data with users being extracted from list
sa.save_data(df_final, Path(DATA_BEFORE_TRAIN_PATH))    # data before train_test_split
sa.save_data(train_test_data[1], Path(TRAIN_DATA_PATH)) # train_data
sa.save_data(train_test_data[2], Path(TEST_DATA_PATH))  # test data

aws.upload_artifacts(aws_access_key, aws_secret_access_key, aws_region, artifacts, config['aws'])
print("Uploaded!")
