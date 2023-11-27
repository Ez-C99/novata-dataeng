import os, sys
import logging
import pandas as pd
import data_processing as dp 
import db_operations as db_ops

from constants import DATA_PATH, STAGING_FOLDER, DB_PATH

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../test/data_quality')))

from test_data_quality import test_data_quality

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Main execution start
def main():

    # Run data quality tests first
    logging.info("Running data quality tests...")
    try:
        test_data_quality() 
    except AssertionError as e: 
        logging.error(f"Data quality tests failed: {e}")
        raise
    logging.info("Data quality tests passed.")

    # Data extraction
    logging.info("Extracting data...")
    try:
        data = dp.extract(DATA_PATH)
    except ValueError as e:
        logging.error(f"Value Error during data extraction: {e}")
        raise
    logging.info("Successfully extracted data")

    # Create snapshot of extracted data
    extracted_snapshot_path = dp.export_snapshot(data, STAGING_FOLDER, 'extracted_data')
    logging.info(f"Snapshot of extracted data snapshot created in staging at {extracted_snapshot_path}")


    # Task 1: Output number of rows
    row_count = len(data)
    logging.info(f"There are {row_count} rows in the original data")


    # Task 2: Data dedupe
    logging.info("Deduplicating data...")
    deduplicated_data = dp.deduplicate(data)
    logging.info("Data deduplication complete")

    # Deduplication comparison
    # dupe_data = pd.merge(data, deduplicated_data, how='left', indicator=True)
    # dupe_data = dupe_data.loc[dupe_data['_merge'] == 'left_only']
    # logging.info(dupe_data)
    logging.info(data[~data.index.isin(deduplicated_data.index)])
    logging.info(data[data['id'] == 'fef784c0-dde6-4e48-b2bb-8f0d4dd62770'])


    # Create snapshot of deduplicated data
    dedupe_snapshot_path = dp.export_snapshot(deduplicated_data, STAGING_FOLDER, 'deduplicated_data')
    logging.info(f"Snapshot of deduplicated data created in staging at {dedupe_snapshot_path}")


    # Task 3: Output number of rows removed
    dropped_row_count = row_count - len(deduplicated_data)
    logging.info(f"There are {dropped_row_count} rows removed")


    # Task 4: Calculate user rank in age group based on user score for age group rank column
    logging.info("Ranking users...")
    ranked_data = dp.rank_users(deduplicated_data)
    logging.info("User ranking complete")


    # Task 5: id, email and age group for top user per age group (ascending)
    logging.info("Retrieving top users per age group...")
    top_user_data = dp.get_top_user_per_age_group(ranked_data)
    logging.info(f"Below are the top users for each age group\n{top_user_data}")


    # Task 6: Flattening the widget list
    logging.info("Flattening widget list...")
    flattened_data = dp.flatten_widget_list(ranked_data)
    logging.info("Widget list flattening complete")


    # Task 7: New total number of rows
    row_count = len(flattened_data)
    logging.info(f"There are currently {row_count} rows in the data")


    # Task 8: add widget name and widget amount columns
    logging.info("Extracting widget info...")
    transformed_data = dp.extract_widget_info(flattened_data)
    logging.info("Widget info extraction complete")
    
    # Create snapshot of transformed data
    transformed_snapshot_path = dp.export_snapshot(transformed_data, STAGING_FOLDER, 'transformed_data')
    logging.info(f"Snapshot of transformed data created in staging at {transformed_snapshot_path}")

    
    # Task 9: Store table in SQLite database (loading from snapshot first)
    logging.info("Loading transformed data from staging...")
    try:
        # Extract the file name from the full path
        file_name = os.path.basename(transformed_snapshot_path)
        transformed_data = dp.load_from_staging(STAGING_FOLDER, file_name)
    except Exception as e:
        logging.error(f"ERROR! Unable to load transformed data from staging: {e}")
        raise
    logging.info("Successfully loaded transformed data from staging")
    
    logging.info("Loading data into SQLite database...")
    try:
        transformed_data = dp.convert_unsupported_data_types(transformed_data)
        db_ops.load(transformed_data, DB_PATH, table_name='transformed_data')
    except Exception as e:
        logging.error(f"ERROR! Unable to load data into database: {e}")
        raise
    logging.info("Successfully loaded data into database")


    # Task 10: Create inverted index dataset
    logging.info("Creating inverted index dataset...")
    try:
        inverted_index = db_ops.create_inverted_index(transformed_data)
    except db_ops.IndexCreationError as e: 
        logging.error(f"Index Creation Error: {e}")
        raise
    logging.info("Successfully created inverted index")

    # Create snapshot of inverted index table
    inverted_snapshot_path = dp.export_snapshot(inverted_index, STAGING_FOLDER, 'inverted_index')
    logging.info(f"Snapshot of inverted index data created in staging at {inverted_snapshot_path}")


    # Task 11: Store inverted index table
    logging.info("Storing inverted index table...")
    try:
        db_ops.store_inverted_index(inverted_index, DB_PATH, table_name='inverted_index')
    except db_ops.IndexStorageError as e:
        logging.error(f"Index Storage Error: {e}")
        raise
    logging.info("Successfully stored inverted index table")


if __name__ == '__main__':
    main()