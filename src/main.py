import logging

import data_processing as dp 
import db_operations as db_ops

from constants import DATA_PATH, STAGING_FOLDER, EXPORT_FOLDER, DB_PATH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Main execution start
def main():
    # Data extraction
    logging.info("Extracting data...")
    try:
        data = dp.extract(DATA_PATH)
    except Exception as e:
        logging.error(f"ERROR! Unable to extract data: {e}")
        raise

    logging.info("Successfully extracted data")


    # Task 1: Output number of rows
    row_count = len(data)
    logging.info(f"There are {row_count} rows in the original data")


    # Task 2: Data dedupe
    logging.info("Deduplicating data...")
    deduplicated_data = dp.deduplicate(data)
    logging.info("Data deduplication complete")


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


    # Task 9: Store table in SQLite database
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
    except Exception as e:
        logging.error(f"ERROR! Unable to create inverted index: {e}")
        raise

    logging.info("Successfully created inverted index")


    # Task 11: Store inverted index table
    logging.info("Storing inverted index table...")
    try:
        db_ops.store_inverted_index(inverted_index, DB_PATH, table_name='inverted_index')
    except Exception as e:
        logging.error(f"ERROR! Unable to store inverted index table: {e}")
        raise

    logging.info("Successfully stored inverted index table")


if __name__ == '__main__':
    main()