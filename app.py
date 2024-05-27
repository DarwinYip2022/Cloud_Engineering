"""
Recommender System Interface
This Streamlit app allows users to generate recommendations using 
collaborative filtering and content-based filtering models.
"""

import os
from pathlib import Path
import pickle
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from  src.project_pipeline.aws_utils import load_from_s3
import src.project_pipeline.load_config as lc

# Load configuration and environment variables
load_dotenv()
CONFIG_PATH = os.getenv("CONFIG_PATH", "config/default.yaml")
config = lc.load_config(Path(CONFIG_PATH))

load_dotenv()
aws_access_key = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
aws_region = os.getenv("aws_region")
bucket_name = config["aws"]["bucket_name"]

@st.cache_resource
def load_model(model_path):
    """
    Load a machine learning model from the specified path.

    Parameters:
    - model_path (str): The path to the model file.

    Returns:
    - model: The loaded machine learning model.
    """
    with open(model_path, 'rb') as file:
        return pickle.load(file)

def load_data(data_path):
    """
    Load data from the specified path.

    Parameters:
    - data_path (str): The path to the data file.

    Returns:
    - pd.DataFrame: The loaded data.
    """
    return pd.read_pickle(data_path)

def make_content_based_predictions(user_id, df_with_one_hot, pipeline):
    """
    Generate content-based recommendations for a given user.

    Parameters:
    - user_id (str): The ID of the user for whom recommendations are to be generated.
    - df_with_one_hot (pd.DataFrame): The dataframe containing one-hot encoded data.
    - pipeline: The trained content-based filtering pipeline.

    Returns:
    - list: A list of recommendation dictionaries, 
    each containing 'product_id' and 'predicted_rating'.
    """
    all_product_ids = df_with_one_hot['product_id'].unique()

    user_interactions = df_with_one_hot[df_with_one_hot['user_id'] == user_id]['product_id'].values
    unrated_product_ids = [product_id for product_id in all_product_ids
                            if product_id not in user_interactions]

    x_new = df_with_one_hot[df_with_one_hot['product_id'].isin(unrated_product_ids)]
    x_new = x_new.drop(columns=['rating', 'user_id'])

    predictions = pipeline.predict(x_new)

    predicted_ratings = pd.DataFrame({'product_id': x_new['product_id'],
                                       'predicted_rating': predictions})
    predicted_ratings.sort_values(by='predicted_rating', ascending=False, inplace=True)
    predicted_ratings = predicted_ratings.drop_duplicates(subset=['product_id'])

    num_recs = 10
    recommendations = []
    for _, row in predicted_ratings.head(num_recs).iterrows():
        recommendation = {
            "product_id": row['product_id'],
            "predicted_rating": row['predicted_rating']
        }
        recommendations.append(recommendation)

    return recommendations

def generate_cf_recommendations(model, user_id, df_with_one_hot):
    """
    Generates collaborative filtering recommendations based on the selected model and user input.

    Parameters:
    - model: The collaborative filtering model.
    - user_id (str): The ID of the user for whom recommendations are to be generated.
    - df_with_one_hot (pd.DataFrame): The dataframe containing one-hot encoded data.

    Returns:
    None
    """
    predictions = []
    for product_id in df_with_one_hot['product_id'].unique():
        pred = model.predict(uid=user_id, iid=str(product_id))
        predictions.append((product_id, pred.est))



    predicted_ratings = pd.DataFrame(predictions, columns=['product_id', 'predicted_rating'])
    predicted_ratings.sort_values(by='predicted_rating', ascending=False, inplace=True)
    predicted_ratings = predicted_ratings.drop_duplicates(subset=['product_id'])

    num_recs = 10
    top_recommendations = predicted_ratings.head(num_recs)
    st.write(f"Top {num_recs} recommendations for user {user_id}:")
    st.dataframe(top_recommendations)


def generate_cbf_recommendations(user_id, df_with_one_hot):
    """
    Generates content-based filtering recommendations based on the selected model and user input.

    Parameters:
    - user_id (str): The ID of the user for whom recommendations are to be generated.
    - df_with_one_hot (pd.DataFrame): The dataframe containing one-hot encoded data.

    Returns:
    None
    """
    content_based_model_path = Path('artifacts/Content_Based_Filtering/best_cbf.pkl')
    if not content_based_model_path.exists():
        st.error(f"Model file not found: {content_based_model_path}")
        return

    content_based_pipeline = load_model(content_based_model_path)
    st.write("Content Based Filtering model loaded successfully!")
    recommendations = make_content_based_predictions(user_id, df_with_one_hot,
                                                     content_based_pipeline)
    num_recs = 10
    st.write(f"Top {num_recs} recommendations for user {user_id}:")
    st.write(pd.DataFrame(recommendations))


def generate_recommendations(model_choice, user_id, model, df_with_one_hot):
    """
    Generates recommendations based on the selected model and user input.

    Parameters:
    - model_choice (str): The choice of model for generating recommendations.
    - user_id (str): The ID of the user for whom recommendations are to be generated.
    - model: The collaborative filtering model object.
    - df_with_one_hot (pd.DataFrame): The dataframe containing one-hot encoded data.

    Returns:
    None
    """
    if model_choice == 'Collaborative Filtering':
        generate_cf_recommendations(model, user_id, df_with_one_hot)
    elif model_choice == 'Content Based Filtering':
        generate_cbf_recommendations(user_id, df_with_one_hot)



def main():
    """
    Main function to run the Streamlit app.
    """
    st.title('Recommender System Interface')

    data_path = Path("artifacts/Data/final_df.pkl")
    if data_path.exists():
        df_with_one_hot = load_data(data_path)
    else:
        st.error("Data file not found. Please check your setup.")
        return

    if st.button('Download Artifacts from S3'):
        target_directories = ['artifacts_Collaborative_Filtering',
                        'artifacts_Content_Based_Filtering',
                              'artifacts_Data']
        load_from_s3(aws_access_key, aws_secret_access_key,
                     aws_region, bucket_name, target_directories)
        st.session_state['models_downloaded'] = True

    # Check if models are downloaded before proceeding
    if st.session_state.get('models_downloaded', False):
        model_choice = st.selectbox('Select Model',
                                    ['Collaborative Filtering', 'Content Based Filtering'])
        model_paths = {
            'Collaborative Filtering': Path('artifacts/Collaborative_Filtering/best_cf.pkl'),
            'Content Based Filtering': Path('artifacts/Content_Based_Filtering/best_cbf.pkl')
        }
        if model_paths[model_choice].exists():
            model = load_model(model_paths[model_choice])
            st.write(f"{model_choice} model loaded successfully!")
        else:
            st.error(f"Model file not found: {model_paths[model_choice]}")
            return

        user_id = st.text_input('Enter User ID:')
        if st.button('Generate Recommendations'):
            if user_id:
                generate_recommendations(model_choice, user_id, model, df_with_one_hot)
            else:
                st.error("Please enter a valid User ID.")



if __name__ == "__main__":
    main()
