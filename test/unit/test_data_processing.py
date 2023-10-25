import os, sys
import pytest
import pandas as pd
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

import data_processing as dp
from constants import DATA_PATH

logging.basicConfig(level=logging.INFO)

@pytest.fixture(scope='module')
def sample_data():
    """Fixture to provide sample data for testing."""
    return pd.DataFrame({'A': [1, 2, 3]})


@pytest.fixture(scope='module')
def complex_data():
    """Fixture to provide complex data for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4],
        'email': ['a@a.com', 'b@b.com', 'c@c.com', 'd@d.com'],
        'age_group': [1, 2, 1, 2],
        'user_score': [5, 3, 8, 6]
    })


def test_extract():
    """
    Test the extract function from the dp module.

    Tests include:
    1. Successful extraction from a valid file path.
    2. Handling of a nonexistent file path.
    """
    logging.info("Starting test_extract...")

    assert os.path.exists(DATA_PATH), f"Data path {DATA_PATH} does not exist"
    data = dp.extract(data_path=DATA_PATH)
    assert isinstance(data, pd.DataFrame)
    assert not data.empty

    # Test unsuccessful extraction
    with pytest.raises(ValueError, match="Failed to load data from nonexistent_path"):
        dp.extract(data_path="nonexistent_path")

    logging.info("test_extract completed successfully.")


def test_export_snapshot(tmp_path, sample_data):
    """
    Test the export_snapshot function from the dp module.

    Tests include:
    1. Normal operation to ensure data is exported and saved to the specified file path.
    """
    logging.info("Starting test_export_snapshot...")

    snapshot_name = 'test_snapshot'
    snapshot_path = dp.export_snapshot(sample_data, tmp_path, snapshot_name)
    assert os.path.exists(snapshot_path)

    logging.info("test_export_snapshot completed successfully.")

def test_load_from_staging(tmp_path, sample_data):
    """
    Test the load_from_staging function from the dp module.

    Tests include:
    1. Normal operation to ensure data is loaded correctly from the specified file path.
    """
    logging.info("Starting test_load_from_staging...")

    file_name = 'test_data.csv'
    file_path = tmp_path / file_name
    sample_data.to_csv(file_path, index=False)
    loaded_data = dp.load_from_staging(str(tmp_path), file_name)
    pd.testing.assert_frame_equal(sample_data, loaded_data)

    logging.info("test_load_from_staging completed successfully.")

def test_deduplicate(complex_data):
    """
    Test the deduplicate function from the dp module.

    Tests include:
    1. Normal operation to ensure data is deduplicated based on 'id' and 'created_at'.
    2. Handling of empty input data.
    """
    logging.info("Starting test_deduplicate...")

    # Include some duplicate rows based on 'id' and 'created_at'
    data = pd.DataFrame({'id': [1, 1, 2, 2], 'created_at': ['2021-01-01', '2021-01-01', '2021-01-03', '2021-01-03']})
    deduplicated_data = dp.deduplicate(data)
    assert len(deduplicated_data) == 2  # Now there should be 2 unique rows

    # Test with empty DataFrame
    with pytest.raises(ValueError, match="Input DataFrame is empty"):
        dp.deduplicate(pd.DataFrame())

    logging.info("test_deduplicate completed successfully.")


def test_rank_users(complex_data):
    """
    Test the rank_users function from the dp module.
    
    Tests include:
    1. Normal operation.
    2. Handling of empty input data.
    """
    logging.info("Starting test_rank_users...")

    # Create a DataFrame with known values
    data = pd.DataFrame({
        'user_id': [1, 2, 3, 4],
        'age_group': ['A', 'A', 'B', 'B'],
        'user_score': [30, 40, 20, 10]
    })

    # Call the rank_users function
    ranked_data = dp.rank_users(data)

    # Check the 'age_group_rank' column is present
    assert 'age_group_rank' in ranked_data.columns

    # Sort the data by age_group and user_score to ensure consistency
    ranked_data.sort_values(by=['age_group', 'user_score'], ascending=[True, False], inplace=True)

    # Check the rank within each age group is as expected
    assert list(ranked_data['age_group_rank']) == [1, 2, 1, 2]

    # Test with empty DataFrame
    with pytest.raises(ValueError, match="Input DataFrame is empty"):
        dp.rank_users(pd.DataFrame())

    logging.info("test_rank_users completed successfully.")


def test_get_top_user_per_age_group(complex_data):
    """
    Test the get_top_user_per_age_group function from dp module.
    
    Tests include:
    1. Normal operation.
    2. Handling of missing required columns.
    3. Handling of empty input data.
    """
    logging.info("Starting test_get_top_user_per_age_group...")

    top_users = dp.get_top_user_per_age_group(complex_data)
    assert list(top_users['id']) == [3, 4]

    # Test missing required columns
    data = complex_data.drop(columns=['user_score'])
    with pytest.raises(ValueError, match="Missing required column(s):"):
        dp.get_top_user_per_age_group(data)

    # Test with empty DataFrame
    with pytest.raises(ValueError, match="Input DataFrame is empty"):
        dp.get_top_user_per_age_group(pd.DataFrame())

    logging.info("test_get_top_user_per_age_group completed successfully.")

def test_flatten_widget_list():
    """
    Test the flatten_widget_list function from dp module.
    
    Tests include:
    1. Normal operation.
    2. Handling of empty input data.
    """
    logging.info("Starting test_flatten_widget_list...")

    data = pd.DataFrame({'widget_list': [[{'name': 'widget1'}], [{'name': 'widget2'}]]})
    flattened_data = dp.flatten_widget_list(data)
    assert len(flattened_data) == 2

    # Test with empty DataFrame
    with pytest.raises(ValueError, match="Input DataFrame is empty"):
        dp.flatten_widget_list(pd.DataFrame())

    logging.info("test_flatten_widget_list completed successfully.")

def test_convert_unsupported_data_types():
    """
    Test the convert_unsupported_data_types function from dp module.
    
    Tests include:
    1. Normal operation.
    2. Handling of empty input data.
    """
    logging.info("Starting test_convert_unsupported_data_types...")

    data = pd.DataFrame({'list_column': [[1, 2, 3]], 'dict_column': [{'key': 'value'}]})
    converted_data = dp.convert_unsupported_data_types(data)
    assert converted_data['list_column'].dtype == object
    assert converted_data['dict_column'].dtype == object

    # Test with empty DataFrame
    with pytest.raises(ValueError, match="Input DataFrame is empty"):
        dp.convert_unsupported_data_types(pd.DataFrame())

    logging.info("test_convert_unsupported_data_types completed successfully.")

def test_extract_widget_info():
    """
    Test the extract_widget_info function from dp module.
    
    Tests include:
    1. Normal operation.
    2. Handling of empty input data.
    """
    logging.info("Starting test_extract_widget_info...")

    data = pd.DataFrame({'widget_list': [{'name': 'widget1', 'amount': 10}]})
    extracted_data = dp.extract_widget_info(data)
    assert 'widget_name' in extracted_data.columns
    assert 'widget_amount' in extracted_data.columns

    # Test with empty DataFrame
    with pytest.raises(ValueError, match="Input DataFrame is empty"):
        dp.extract_widget_info(pd.DataFrame())

    logging.info("test_extract_widget_info completed successfully.")

if __name__ == "__main__":
    pytest.main()
