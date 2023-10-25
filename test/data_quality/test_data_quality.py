import os, sys
import great_expectations as ge
import pytest
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

import data_processing as dp
from constants import DATA_PATH
from profiler import profile_data 

logging.basicConfig(level=logging.INFO)

def test_data_quality(data_path=DATA_PATH):
    """
    Validate the quality of data processed by the data_processing module
    using the Great Expectations library.
    """
    logging.info("Starting data quality test...")

    try:
        data = dp.extract(data_path=data_path)
        ge_data = ge.from_pandas(data)
    except Exception as e:
        pytest.fail(f"Data extraction or conversion failed: {e}")

    # Get the auto-generated expectation suite
    expectation_suite = profile_data(data_path)

    # Validate data against the expectations suite
    results = ge_data.validate(expectation_suite=expectation_suite)

    # Assert that all expectations are met
    assert results["success"] == True, f"Data quality test failed: {results}"
    
    logging.info("Data quality test completed successfully.")

if __name__ == "__main__":
    pytest.main()
