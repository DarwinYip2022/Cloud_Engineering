def make_content_based_predictions(user_id, df_new, content_based_pipeline, df_with_one_hot):
    if not df_new.empty:
        # Check if user_id exists in df_new
        if user_id in df_new['user_id'].unique():
            user_name = df_new.loc[df_new['user_id'] == user_id, 'user_name'].iloc[0]
            # Rest of the code to generate recommendations
        else:
            # Handle case when user_id does not exist
            return f"User ID {user_id} not found in the dataset."
    else:
        # Handle case when DataFrame is empty
        return "DataFrame is empty."
