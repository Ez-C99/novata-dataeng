import great_expectations as ge
import pytest
import logging

from src import data_processing as dp
from constants import DATA_PATH

logging.basicConfig(level=logging.INFO)


def get_expectation_suite():
    """
    Returns the expectation suite for data validation.
    """
    expectation_suite = {
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 1,
                    "max_value": 10000
                }
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "age_group"
                }
            },
            {
                "expectation_type": "expect_column_median_to_be_between",
                "kwargs": {
                    "column": "user_score",
                    "min_value": 0,
                    "max_value": 10
                }
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "email"
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "age_group",
                    "value_set": ['18-24', '25-34', '35-44', '45-54', '55-64', '65+']
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "revenue",
                    "min_value": 0,
                    "max_value": 10000
                }
            },
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "location",
                    "regex": ".+,.+"
                }
            },
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "creation_date",
                    "regex": r"\d{4}-\d{2}-\d{2}"
                }
            },
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "email",
                    "regex": r"[^@]+@[^@]+\.[^@]+"
                }
            }
        ]
    }
    return expectation_suite


def test_data_quality(data_path=DATA_PATH, expectation_suite=None):
    """
    Validate the quality of data processed by the data_processing module
    using the Great Expectations library.
    
    This test ensures that the data extracted from the specified data path:
    1. Contains between 1 and 10,000 rows.
    2. Has non-null values in the 'age_group' and 'email' columns.
    3. Has 'user_score' column values with a median between 0 and 10.
    4. Contains only specified age groups in the 'age_group' column.
    5. Has 'revenue' column values between 0 and 10,000.
    6. Has 'location' and 'creation_date' column values matching specified regex patterns.
    7. Has 'email' column values matching a valid email regex pattern.
    """
    logging.info("Starting data quality test...")

    if expectation_suite is None:
        expectation_suite = get_expectation_suite()
    
    try:
        data = dp.extract(data_path=data_path)
        ge_data = ge.from_pandas(data)
    except Exception as e:
        pytest.fail(f"Data extraction or conversion failed: {e}")

    # Validate data against the expectations suite
    results = ge_data.validate(expectation_suite=expectation_suite)

    # Assert that all expectations are met
    assert results["success"] == True, f"Data quality test failed: {results}"
    
    logging.info("Data quality test completed successfully.")


if __name__ == "__main__":
    pytest.main()