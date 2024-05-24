from typing import Tuple
import pandas as pd


def data_preprocess(df: pd.DataFrame):

    df.fillna(value=pd.NA, inplace=True)  # Fill missing values with 'NA'
    df['discounted_price'] = df['discounted_price'].str.replace('₹', '').str.replace(',', '').astype(float)
    df['actual_price'] = df['actual_price'].str.replace('₹', '').str.replace(',', '').astype(float)

    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df.dropna(inplace=True)

    df['rating_count'] = df['rating_count'].str.replace(',', '').astype(int)
    df['product_name'] = df['product_name'].str.lower()

    df['discount_percentage'] = df['discount_percentage'].str.rstrip('%').astype(float)

    df['about_product'] = df['about_product'].str.replace('[^\w\s]', '').str.lower()
    df['review_title'] = df['review_title'].str.replace('[^\w\s]', '').str.lower()
    df['review_content'] = df['review_content'].str.replace('[^\w\s]', '').str.lower()

    return df

def split_users(row):
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

def extract_first_last(category) -> Tuple:
    categories = category.split('|')
    first_item = categories[0]
    last_item = categories[-1]
    return first_item, last_item

def one_hot_encoding(df: pd.DataFrame) -> pd.DataFrame:
    df.drop(columns=['product_name','img_link','product_link'],inplace=True)
    one_hot_encoded = pd.get_dummies(df['First_category'], prefix='first_category')

    # Concatenate one-hot encoded columns with the original dataframe
    df_with_one_hot = pd.concat([df.drop(columns=['First_category','Last_category','rating_count',\
                                                  'review_id','about_product','actual_price','review_content']),\
                                                  one_hot_encoded], axis=1)
    return df_with_one_hot
