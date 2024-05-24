from typing import List, Union
from surprise.model_selection import cross_validate
import pandas as pd
from sklearn.model_selection import train_test_split
from surprise import Dataset, Reader, SVD
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
from sklearn.feature_extraction.text import TfidfVectorizer



def train_test_data(df:pd.DataFrame, test_size: float, random_state: int, training_col: List[str]):
    train_data, test_data = train_test_split(df, test_size = test_size, random_state=random_state)
    reader = Reader(rating_scale=(0,5))
    data_train_collab = Dataset.load_from_df(train_data[training_col],reader)
    return (data_train_collab, train_data, test_data)

def collaborative_filtering(df: pd.DataFrame, n_factors: List[int], lr_all: List[float], reg_all: List[float]):

    param_grid = {
        'n_factors': n_factors,
        'lr_all': lr_all,
        'reg_all': reg_all
    }
    rmse_results_svd = {}

    for n_factors in param_grid['n_factors']:
        for lr_all in param_grid['lr_all']:
            for reg_all in param_grid['reg_all']:
                # Set algorithm parameters
                algo = SVD(n_factors=n_factors, lr_all=lr_all, reg_all=reg_all, verbose=False)

                # Perform cross-validation
                cv_results = cross_validate(algo, df, measures=['RMSE'], cv=5, verbose=False)

                # Calculate mean RMSE across folds
                mean_rmse = cv_results['test_rmse'].mean()

                # Store the RMSE for this parameter combination
                rmse_results_svd[(n_factors, lr_all, reg_all)] = mean_rmse

    # Find the parameter combination with the least RMSE
    best_params_svd = min(rmse_results_svd, key=rmse_results_svd.get)
    #best_rmse_svd = rmse_results_svd[best_params_svd]

    # Re-train the best model on the full dataset
    best_model = SVD(n_factors=best_params_svd[0], lr_all=best_params_svd[1], reg_all=best_params_svd[2])
    trainset = df.build_full_trainset()
    best_model.fit(trainset)

    return best_model

def content_base_filtering(numeric_features: List[str], text_feature: Union[str,List[str]], train_data):
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

    X_train = train_data.drop(columns=['rating'])
    y_train = train_data['rating']

    # Train the model
    pipeline.fit(X_train, y_train)

    return pipeline
