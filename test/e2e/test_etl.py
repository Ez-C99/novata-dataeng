import pytest
import os
import sys
import pandas as pd
import sqlite3
import logging

# Append the path to access the modules from the src directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from main import main
from constants import DB_PATH

# Configure logging for testing
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@pytest.fixture(scope='module')
def setup_teardown():
    """
    A fixture to setup necessary configurations before running E2E tests,
    and to tear down any configurations after tests have completed.
    """
    logging.info("Setting up for ETL tests")
    # You could include setup activities here like creating a test database
    yield  # This will execute the tests
    logging.info("Tearing down after ETL tests")
    # You could include teardown activities here like deleting the test database

def test_etl_process(setup_teardown):
    """
    Test the entire ETL process by running the main function,
    loading the resulting data from the database,
    and asserting the final state of the data against hypothetical expected data.
    """
    logging.info("Testing entire ETL process")
    try:
        main()
    except Exception as e:
        logging.error(f"Error encountered during ETL process: {e}")
        raise  # Re-raise the exception to fail the test

    # Hypothetical block to check the final state of the data with pre-defined expected data
    try:
        # Load the resulting data from the database for assertion against hypothetical expected data
        with sqlite3.connect(DB_PATH) as conn:
            transformed_data = pd.read_sql('SELECT * FROM transformed_data', conn)
            inverted_index = pd.read_sql('SELECT * FROM inverted_index', conn)

        # In the presence of pre-defined expected data loaded from files
        # expected_transformed_data = pd.read_csv('expected_transformed_data.csv')
        # expected_inverted_index = pd.read_csv('expected_inverted_index.csv')

        # Assert the final state of the data against hypothetical expected data
        # pd.testing.assert_frame_equal(transformed_data, expected_transformed_data)
        # pd.testing.assert_frame_equal(inverted_index, expected_inverted_index)
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise  # Re-raise the exception to fail the test
    except FileNotFoundError as e:
        logging.error(f"File not found error: {e}")
        raise  # Re-raise the exception to fail the test
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise  # Re-raise the exception to fail the test

if __name__ == "__main__":
    pytest.main()
