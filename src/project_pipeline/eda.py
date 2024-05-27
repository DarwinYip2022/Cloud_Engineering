""" Module to perform EDA"""
from typing import Tuple
import pandas as pd


def data_preprocess(data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the input DataFrame.

    Args:
        data (pd.DataFrame): Input DataFrame to be preprocessed.

    Returns:
        pd.DataFrame: Preprocessed DataFrame.
    """
    data.fillna(value=pd.NA, inplace=True)  # Fill missing values with 'NA'
    data['discounted_price'] = data['discounted_price'].str.replace(
        '₹', '').str.replace(',', '').astype(float)
    data['actual_price'] = data['actual_price'].str.replace(
        '₹', '').str.replace(',', '').astype(float)

    data['rating'] = pd.to_numeric(data['rating'], errors='coerce')
    data.dropna(inplace=True)

    data['rating_count'] = data['rating_count'].str.replace(',', '').astype(int)
    data['product_name'] = data['product_name'].str.lower()

    data['discount_percentage'] = data['discount_percentage'].str.rstrip('%').astype(float)

    data['about_product'] = data['about_product'].str.replace(r'[^\w\s]', '').str.lower()
    data['review_title'] = data['review_title'].str.replace(r'[^\w\s]', '').str.lower()
    data['review_content'] = data['review_content'].str.replace(r'[^\w\s]', '').str.lower()

    return data


def split_users(row: pd.Series) -> pd.DataFrame:
    """Splits user information in a DataFrame row and creates new rows for each user.

    Args:
        row (pd.Series): Input row containing user information.

    Returns:
        pd.DataFrame: DataFrame with each user information split into separate rows.
    """
    user_ids = row['user_id'].split(',')
    user_names = row['user_name'].split(',')
    review_titles = row['review_title'].split(',')
    rows = []
    for uid, uname, title in zip(user_ids, user_names, review_titles):
        row_copy = row.copy()
        row_copy['user_id'] = uid
        row_copy['user_name'] = uname
        row_copy['review_title'] = title
        rows.append(row_copy)
    return pd.DataFrame(rows)


def extract_first_last(category: str) -> Tuple[str, str]:
    """Extracts the first and last items from a string of categories separated by '|'.

    Args:
        category (str): String containing categories separated by '|'.

    Returns:
        Tuple[str, str]: Tuple containing the first and last categories.
    """
    categories = category.split('|')
    first_item = categories[0]
    last_item = categories[-1]
    return first_item, last_item


def one_hot_encoding(data: pd.DataFrame) -> pd.DataFrame:
    """Performs one-hot encoding on the 'First_category' column of the DataFrame.

    Args:
        data (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with one-hot encoded 'First_category' column.
    """
    data.drop(columns=['product_name', 'img_link', 'product_link'], inplace=True)
    one_hot_encoded = pd.get_dummies(data['First_category'], prefix='first_category')

    # Concatenate one-hot encoded columns with the original dataframe
    data_with_one_hot = pd.concat([data.drop(columns=['First_category',
                                                      'Last_category',
                                                      'rating_count',
                                                    'review_id',
                                                    'about_product',
                                                    'actual_price',
                                                    'review_content']),
                                 one_hot_encoded], axis=1)
    return data_with_one_hot
