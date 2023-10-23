import os

# Absolute path to the project_root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Path to the raw data, staging folder, and export folder
DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'data.json')
STAGING_FOLDER = os.path.join(PROJECT_ROOT, 'data', 'staging')
EXPORT_FOLDER = os.path.join(PROJECT_ROOT, 'data', 'export')

# Path to the SQLite database within the export folder
DB_PATH = os.path.join(EXPORT_FOLDER, 'database.db')