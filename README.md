# Ezra Chamba Novata Data Engineer Challenge

## Description

This project is an exercise to demonstrate design, implementation, and data engineering skills. It consists of transforming a given JSON data file (`data.json`) which contains user activity records. The project aims to process and store the data, following specified requirements including data deduplication, ranking users within their age groups, flattening nested data structures, and storing the transformed data in a SQLite database.

## Requirements

- Python 3.10

## Libraries

- pandas
- great_expectations
- pytest
- sqlite3

## Installation and Setup

1. Clone the repository:

```bash
git clone https://github.com/Ez-C99/novata-dataeng
```

2. Navigate to the main directory:

```bash
cd novata-dataeng
```

3. Install the necessary libraries using pip:

```bash
pip3 install -r requirements.txt
```

## Usage

To run the main ETL script, navigate to the src directory and execute the main.py script:

```bash
cd src
python3 main.py
```

## Directory Structure

Below is the structure of the project which organises the code, tests, and data systematically for ease of understanding and usage:

```
novata-dataeng
├─ README.md
├─ data
│  ├─ export
│  ├─ raw
│  │  └─ data.json
│  └─ staging
├─ requirements.txt
├─ src
│  ├─ constants.py
│  ├─ data_processing.py
│  ├─ db_operations.py
│  └─ main.py
└─ test
   ├─ data_quality
   │  ├─ profiler.py
   │  └─ test_data_quality.py
   ├─ e2e
   │  └─ test_etl.py
   └─ unit
      └─ test_data_processing.py

```

## Data Flow

1. **Extraction**: Raw data is extracted from `data/raw/data.json`.
2. **Staging**: Data is staged in the `data/staging` directory for processing.
3. **Transformation**: Various transformations including deduplication, ranking, and flattening are performed.
4. **Loading**: Transformed data is loaded into a local SQLite database and exported to the `data/export` directory.
5. **Testing**: Data quality, unit, and end-to-end tests are conducted to ensure the integrity and correctness of the ETL process.

## Features

The project is structured to complete the following tasks as per the given challenge:

1. Count and output the number of rows in the original input file.
2. Deduplicate the original data using the id and created_at columns.
3. Output the number of rows removed after deduplication.
4. Calculate and store the rank of users within their age group based on their user_score.
5. Output the top user per age group.
6. Flatten the widget_list column to generate additional rows.
7. Output the new total number of rows post-transformation.
8. Extract widget_name and widget_amount from the widget_list column and store them as new columns.
9. Store the transformed data in a local SQLite database.
10. Create an inverted index dataset based on the location column.
11. Store the inverted index table in the SQLite database.

## Testing

### Data Quality

Data quality testing is integrated to ensure the integrity of the data at the beginning of the ETL script. The test_data_quality.py script inside the test/data_quality/ directory is used for this purpose. The script utilizes the Great Expectations library to validate the data against a defined set of expectations.

To run data quality tests independently, use the following command from the main directory:

```bash
python3 -m pytest test/data_quality/
```

### Unit Testing

Unit tests are created to ensure that the functions within the data_processing module are working as expected. The test_data_processing.py script inside the test/ directory contains the unit tests for various functions such as data extraction, deduplication, ranking, and others.

To run unit tests, use the following command from the main directory:

```bash
python3 -m pytest test/test_data_processing.py
```

### End-to-End System Testing

End-to-end testing is performed to validate the entire ETL process from start to finish. The test_etl.py script inside the test/ directory encapsulates the E2E testing, ensuring that the entire process runs smoothly and results in the expected transformations and data storage.

To run end-to-end tests, use the following command from the main directory:

```bash
python3 -m pytest test/test_etl.py
```

## Additional Notes

- The `main.py` script in the `src` directory orchestrates the ETL process. It ensures data quality before proceeding with the rest of the ETL tasks.
- The `requirements.txt` file lists all the necessary libraries and dependencies for the project. Ensure to install them using pip3 as outlined in the Installation and Setup section.
- Data quality tests are designed to catch unexpected data quality issues before impacting downstream tasks, thereby ensuring the integrity of the data at the onset of the ETL process.
- The SQLite database and exported snapshots provide persistent storage solutions, making it easy to inspect the transformed data and the inverted index table.

To ensure smooth execution, it's advisable to follow the instructions under the Installation and Setup, and Usage sections. Make sure all dependencies are installed, and the directory structure is intact as per the project tree.
