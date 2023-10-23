import pandas as pd

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

def deduplicate(data):
    """
    Deduplicate the data based on 'id' and 'created_at'.
    
    Parameters:
    data (pd.DataFrame): The input data.
    
    Returns:
    pd.DataFrame: The deduplicated data.
    """
    if data.empty:
        raise ValueError("Input data is empty")
    
    deduplicated_data = data.drop_duplicates(subset=['id', 'created_at'])
    return deduplicated_data

def rank_users(data):
    """
    Rank the users within their age groups based on their user_score.
    
    Parameters:
    data (pd.DataFrame): The input data.
    
    Returns:
    pd.DataFrame: The data with an added 'age_group_rank' column.
    """
    if data.empty:
        raise ValueError("Input data is empty")

    data['age_group_rank'] = data.sort_values('user_score', ascending=False)\
                                  .groupby('age_group')\
                                  .cumcount() + 1
    return data

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
    required_columns = ['age_group', 'user_score', 'id', 'email']
    if not all(col in data.columns for col in required_columns):
        raise ValueError(f"Missing required columns: {', '.join(required_columns)}")
    if data.empty:
        raise ValueError("Input data is empty")

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
    """
    if data.empty:
        raise ValueError("Input data is empty")

    data_exploded = data.explode('widget_list')
    return data_exploded

def convert_unsupported_data_types(data):
    """
    Convert unsupported data types in a DataFrame to strings.

    Parameters:
    data (pd.DataFrame): The input data.

    Returns:
    pd.DataFrame: The data with unsupported data types converted to strings.
    """
    converted_data = data.applymap(lambda x: str(x) if isinstance(x, (list, dict)) else x)
    return converted_data

def extract_widget_info(data):
    """
    Extract widget_name and widget_amount from the widget_list column.
    
    Parameters:
    data (pd.DataFrame): The input data.
    
    Returns:
    pd.DataFrame: The data with added 'widget_name' and 'widget_amount' columns.
    """
    if data.empty:
        raise ValueError("Input data is empty")

    data['widget_name'] = data['widget_list'].apply(lambda x: x['name'] if isinstance(x, dict) else None)
    data['widget_amount'] = data['widget_list'].apply(lambda x: x['amount'] if isinstance(x, dict) else None)
    return data
