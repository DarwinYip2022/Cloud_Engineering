""" Module to perform all model processing and training steps"""
from typing import List, Union
import pandas as pd
import xgboost as xgb
from surprise.model_selection import cross_validate
from surprise import Dataset, Reader, SVD
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer


def train_test_data(data:pd.DataFrame,
                    test_size: float,
                    random_state: int,
                    training_col: List[str]):
    """Split the data into training and testing sets.

    Args:
        data (pd.DataFrame): The DataFrame containing the data.
        test_size (float): The proportion of the dataset to include in the test split.
        random_state (int): Controls the shuffling applied to the data before applying the split.
        training_col (List[str]): List of column names to be used for training.

    Returns:
        Tuple: A tuple containing the Surprise Dataset, training DataFrame, and testing DataFrame.
    """
    train_data, test_data = train_test_split(data, test_size = test_size, random_state=random_state)
    reader = Reader(rating_scale=(0,5))
    data_train_collab = Dataset.load_from_df(train_data[training_col],reader)
    return (data_train_collab, train_data, test_data)


def collaborative_filtering(data: pd.DataFrame,
                            n_factors_list: List[int],
                            lr_all_list: List[float],
                            reg_all_list: List[float]):
    """Perform collaborative filtering using Singular Value Decomposition (SVD).

        Args:
            data (pd.DataFrame): The DataFrame containing the user-item interactions.
            n_factors_list (List[int]): The list of numbers of factors to try.
            lr_all_list (List[float]): The list of learning rates for all parameters to try.
            reg_all_list (List[float]): The list of regularization terms for all parameters to try.

        Returns:
            SVD: The trained collaborative filtering model.
        """
    param_grid = {
        'n_factors': n_factors_list,
        'lr_all': lr_all_list,
        'reg_all': reg_all_list
    }
    rmse_results_svd = {}

    for n_factors in param_grid['n_factors']:
        for lr_all in param_grid['lr_all']:
            for reg_all in param_grid['reg_all']:
                # Set algorithm parameters
                algo = SVD(n_factors=n_factors, lr_all=lr_all, reg_all=reg_all, verbose=False)

                # Perform cross-validation
                cv_results = cross_validate(algo, data, measures=['RMSE'], cv=5, verbose=False)

                # Calculate mean RMSE across folds
                mean_rmse = cv_results['test_rmse'].mean()

                # Store the RMSE for this parameter combination
                rmse_results_svd[(n_factors, lr_all, reg_all)] = mean_rmse

    # Find the parameter combination with the least RMSE
    best_params_svd = min(rmse_results_svd, key=rmse_results_svd.get)

    # Re-train the best model on the full dataset
    best_model = SVD(n_factors=best_params_svd[0],
                     lr_all=best_params_svd[1],
                     reg_all=best_params_svd[2])
    trainset = data.build_full_trainset()
    best_model.fit(trainset)

    return best_model


def content_base_filtering(numeric_features: List[str],
                           text_feature: Union[str,List[str]],
                           train_data):
    """Train a content-based filtering model using XGBoost.

    Args:
        numeric_features (List[str]): List of column names for numeric features.
        text_feature (Union[str, List[str]]): Column name(s) for text feature(s).
        train_data (pd.DataFrame): DataFrame containing training data.

    Returns:
        Pipeline: Trained content-based filtering model pipeline.
    """
        # Define preprocessing pipelines for different feature types
    numeric_transformer = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])

    text_transformer = Pipeline(steps=[
        ('tfidf', TfidfVectorizer())
    ])

    # Combine preprocessing pipelines into one ColumnTransformer
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('text', text_transformer, text_feature)
        ])

    # Append XGBoost regressor to the preprocessing pipeline
    xgb_model = xgb.XGBRegressor()

    # Create the full pipeline
    pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('xgb_model', xgb_model)])

    x_train = train_data.drop(columns=['rating'])
    y_train = train_data['rating']

    # Train the model
    pipeline.fit(x_train, y_train)

    return pipeline
