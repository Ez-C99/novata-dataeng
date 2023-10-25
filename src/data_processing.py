import os
import pandas as pd
from datetime import datetime

from constants import DATA_PATH


def extract(data_path=DATA_PATH):
    """
    Load the JSON data into a DataFrame.
    
    Parameters:
    data_path (str): The file path to the JSON data. Defaults to DATA_PATH from constants module.
    
    Returns:
    pd.DataFrame: The loaded data.
    
    Raises:
    ValueError: If the file could not be loaded.
    """
    try:
        data = pd.read_json(data_path, lines=True)
    except Exception as e:
        raise ValueError(f"Failed to load data from {data_path}: {e}")
    return data


def export_snapshot(data, snapshot_folder, snapshot_name):
    """
    Export snapshot of data to the required destination.

    Parameters:
    data (pd.DataFrame): The input data.
    snapshot_folder (str): The path of the folder to save to
    snapshot_name (str): The name to be assigned to the file with a timestamp

    Returns:
    str: The path of the folder the snapshot has been saved to

    Raises:
    IOError: If there is an issue writing to the specified file path.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    snapshot_path = os.path.join(snapshot_folder, f'{snapshot_name}_{timestamp}.csv')
    data.to_csv(snapshot_path, index=False)
    return snapshot_path  # Returning the path can be useful for logging or further processing


def load_from_staging(staging_folder, file_name):
    """
    Load snapshot of data from the required destination.

    Parameters:
    staging_folder (str): The path of the folder to load from.
    file_name (str): The name of the file to load.

    Returns:
    pd.DataFrame: The loaded data.
    """
    file_path = os.path.join(staging_folder, file_name)
    data = pd.read_csv(file_path)
    return data


def deduplicate(data):
    """
    Deduplicate the data based on 'id' and 'created_at'.
    
    Parameters:
    data (pd.DataFrame): The input data.
    
    Returns:
    pd.DataFrame: The deduplicated data.

    Raises:
    ValueError: If the input data is empty.
    """
    if data.empty:
        raise ValueError("Input DataFrame is empty")
    
    deduplicated_data = data.drop_duplicates(subset=['id', 'created_at'])
    return deduplicated_data


def rank_users(data):
    """
    Rank the users within their age groups based on their user_score.
    
    Parameters:
    data (pd.DataFrame): The input data.
    
    Returns:
    pd.DataFrame: The data with an added 'age_group_rank' column.

    Raises:
    ValueError: If the input data is empty.
    """
    if data.empty:
        raise ValueError("Input DataFrame is empty")

    # Make a copy of the data to avoid modifying the original DataFrame
    data_copy = data.copy()

    # Sort the data by age_group and user_score, in descending order for user_score
    data_copy.sort_values(by=['age_group', 'user_score'], ascending=[True, False], inplace=True)

    # Compute the rank within each age group based on user_score
    data_copy['age_group_rank'] = data_copy.groupby('age_group')['user_score'].rank(method='min', ascending=False).astype(int)

    return data_copy


def get_top_user_per_age_group(data):
    """
    Get the top user from each age group based on user_score.
    
    Parameters:
    data (pd.DataFrame): The input data.
    
    Returns:
    pd.DataFrame: A DataFrame with one row per age group, showing the top user's id, email, and age_group.
    
    Raises:
    ValueError: If required columns are missing or the input data is empty.
    """
    if data.empty:
        raise ValueError("Input DataFrame is empty")

    required_columns = ['age_group', 'user_score', 'id', 'email']
    if not all(col in data.columns for col in required_columns):
        raise ValueError(f"Missing required columns: {', '.join(required_columns)}")

    return data.sort_values(by=['age_group', 'user_score'], ascending=[True, False])\
                .groupby('age_group')\
                .head(1)[['id', 'email', 'age_group']]


def flatten_widget_list(data):
    """
    Flatten the widget_list column.
    
    Parameters:
    data (pd.DataFrame): The input data.
    
    Returns:
    pd.DataFrame: The data with the 'widget_list' column exploded.

    Raises:
    ValueError: If the input data is empty.
    """
    if data.empty:
        raise ValueError("Input DataFrame is empty")

    data_exploded = data.explode('widget_list')
    return data_exploded


def convert_unsupported_data_types(data):
    """
    Convert unsupported data types in a DataFrame to strings.

    Parameters:
    data (pd.DataFrame): The input data.

    Returns:
    pd.DataFrame: The data with unsupported data types converted to strings.

    Raises:
    ValueError: If the input data is empty.
    """
    if data.empty:
        raise ValueError("Input DataFrame is empty")

    converted_data = data.applymap(lambda x: str(x) if isinstance(x, (list, dict)) else x)
    return converted_data


def extract_widget_info(data):
    """
    Extract widget_name and widget_amount from the widget_list column.
    
    Parameters:
    data (pd.DataFrame): The input data.
    
    Returns:
    pd.DataFrame: The data with added 'widget_name' and 'widget_amount' columns.

    Raises:
    ValueError: If the input data is empty.
    """
    if data.empty:
        raise ValueError("Input DataFrame is empty")

    data['widget_name'] = data['widget_list'].apply(lambda x: x['name'] if isinstance(x, dict) else None)
    data['widget_amount'] = data['widget_list'].apply(lambda x: x['amount'] if isinstance(x, dict) else None)
    return data