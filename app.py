import streamlit as st
import os
from dotenv import load_dotenv
from pathlib import Path
import pickle
import pandas as pd
from src.project_pipeline.aws_utils import load_from_s3
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
    with open(model_path, 'rb') as file:
        return pickle.load(file)

def load_data(data_path):
    return pd.read_pickle(data_path)

def make_content_based_predictions(user_id, df_with_one_hot, pipeline, df_new):
    # Get user name
    user_name = df_new.loc[df_new['user_id'] == user_id, 'user_name'].iloc[0]

    # Get all unique product IDs from the original dataset
    all_product_ids = df_with_one_hot['product_id'].unique()

    # Filter out products the user has already interacted with in the entire dataset
    user_interactions = df_with_one_hot[df_with_one_hot['user_id'] == user_id]['product_id'].values
    unrated_product_ids = [product_id for product_id in all_product_ids if product_id not in user_interactions]

    # Create a dataset with the features of the unrated items
    X_new = df_with_one_hot[df_with_one_hot['product_id'].isin(unrated_product_ids)]
    X_new = X_new.drop(columns=['rating', 'user_id'])

    # Make predictions using the trained pipeline
    predictions = pipeline.predict(X_new)

    # Combine predictions with item IDs
    predicted_ratings = pd.DataFrame({'product_id': X_new['product_id'], 'predicted_rating': predictions})

    # Sort predictions by predicted rating in descending order
    predicted_ratings.sort_values(by='predicted_rating', ascending=False, inplace=True)

    # Remove duplicates
    predicted_ratings = predicted_ratings.drop_duplicates(subset=['product_id'])

    # Print top N recommendations
    N = 10
    recommendations = []
    for i, row in predicted_ratings.head(N).iterrows():
        recommendation = {
            "product_id": row['product_id'],
            "predicted_rating": row['predicted_rating']
        }
        recommendations.append(recommendation)

    return recommendations


def main():
    st.title('Recommender System Interface')

    # Load and cache data
    data_path = Path("artifacts/Data/final_df.pkl")
    if data_path.exists():
        df_with_one_hot = load_data(data_path)
        all_product_ids = df_with_one_hot['product_id'].unique()
    else:
        st.error("Data file not found. Please check your setup.")

    if st.button('Download Artifacts from S3'):
        target_directories = ['artifacts_Collaborative_Filtering', 'artifacts_Content_Based_Filtering', 'artifacts_Data']
        load_from_s3(aws_access_key, aws_secret_access_key, aws_region, bucket_name, target_directories)
        st.session_state['models_downloaded'] = True
        st.success("Artifacts downloaded successfully!")

    if st.session_state.get('models_downloaded', False):
        model_choice = st.selectbox('Select Model', ['Collaborative Filtering', 'Content Based Filtering'])
        model_paths = {
            'Collaborative Filtering': Path('artifacts/Collaborative_Filtering/best_cf.pkl'),
            'Content Based Filtering': Path('artifacts/Content_Based_Filtering/best_cbf.pkl')
        }
        if model_paths[model_choice].exists():
            model = load_model(model_paths[model_choice])
            st.write(f"{model_choice} model loaded successfully!")
        else:
            st.error(f"Model file not found: {model_paths[model_choice]}")

        user_id = st.text_input('Enter User ID:')
        if st.button('Generate Recommendations'):
            if user_id:
                if model_choice == 'Collaborative Filtering':
                    predictions = []
                    for product_id in all_product_ids:
                        try:
                            pred = model.predict(uid=user_id, iid=str(product_id))
                            predictions.append((product_id, pred.est))
                        except Exception as e:
                            st.error(f"Failed to make prediction for product {product_id}: {e}")

                    predicted_ratings = pd.DataFrame(predictions, columns=['product_id', 'predicted_rating'])
                    predicted_ratings.sort_values(by='predicted_rating', ascending=False, inplace=True)
                    predicted_ratings = predicted_ratings.drop_duplicates(subset=['product_id'])
                    N = 10
                    top_recommendations = predicted_ratings.head(N)
                    st.write(f"Top {N} recommendations for user {user_id}:")
                    st.dataframe(top_recommendations)
                elif model_choice == 'Content Based Filtering':
                    # Load Content-Based Filtering model
                    content_based_model_path = Path('artifacts/Content_Based_Filtering/best_cbf.pkl')
                    if content_based_model_path.exists():
                        content_based_pipeline = load_model(content_based_model_path)
                        st.write("Content Based Filtering model loaded successfully!")
                        recommendations = make_content_based_predictions(user_id, df_with_one_hot, content_based_pipeline, df_with_one_hot)
                        st.write(f"Top 10 recommendations for user {user_id}:")
                        st.write(pd.DataFrame(recommendations))
                    else:
                        st.error(f"Model file not found: {content_based_model_path}")
            else:
                st.error("Please enter a valid User ID.")

if __name__ == "__main__":
    main()
