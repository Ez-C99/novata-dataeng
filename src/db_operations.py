import sqlite3

from constants import DB_PATH

class DatabaseError(Exception):
    """An exception class for database-related errors."""

def connect(db_path=DB_PATH):
    """
    Create a database connection and return the connection object.

    Parameters:
    db_path (str): The path to the SQLite database. Defaults to DB_PATH from constants module.

    Returns:
    sqlite3.Connection: The connection object for the SQLite database.

    Raises:
    DatabaseError: If a database connection error occurs.
    """
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        raise DatabaseError(f"Database connection error: {e}")

def load(data, db_path=DB_PATH, table_name='main_table'):
    """
    Load the data into a SQLite database.

    Parameters:
    data (pd.DataFrame): The data to be loaded.
    db_path (str): The path to the SQLite database. Defaults to DB_PATH from constants module.
    table_name (str): The name of the table to load the data into. Defaults to 'main_table'.

    Raises:
    ValueError: If the data is empty.
    DatabaseError: If a database error occurs.
    """
    if data.empty:
        raise ValueError("Input data is empty")

    conn = connect(db_path)  # Create a database connection
    try:
        data.to_sql(table_name, conn, if_exists='replace', index=False)
    except sqlite3.Error as e:
        raise DatabaseError(f"Database error: {e}")
    finally:
        conn.close()  # Close the database connection

def create_inverted_index(data):
    """
    Create an inverted index.

    Parameters:
    data (pd.DataFrame): The data to create the inverted index from.

    Returns:
    pd.DataFrame: The inverted index as a DataFrame.

    Raises:
    ValueError: If the data is empty or required columns are missing.
    """
    required_columns = ['location', 'id']
    if not all(col in data.columns for col in required_columns):
        raise ValueError(f"Missing required columns: {', '.join(required_columns)}")
    if data.empty:
        raise ValueError("Input data is empty")

    inverted_index = data.groupby('location')['id'].apply(lambda x: ','.join(map(str, x))).reset_index()
    return inverted_index

def store_inverted_index(inverted_index, db_path=DB_PATH, table_name='inverted_index'):
    """
    Store the inverted index in a SQLite database.

    Parameters:
    inverted_index (pd.DataFrame): The inverted index as a DataFrame.
    db_path (str): The path to the SQLite database. Defaults to DB_PATH from constants module.
    table_name (str): The name of the table to store the inverted index. Defaults to 'inverted_index'.

    Raises:
    DatabaseError: If a database error occurs.
    """
    conn = connect(db_path)  # Create a database connection
    try:
        inverted_index.to_sql(table_name, conn, if_exists='replace', index=False)
    except sqlite3.Error as e:
        raise DatabaseError(f"Database error: {e}")
    finally:
        conn.close()  # Close the database connection