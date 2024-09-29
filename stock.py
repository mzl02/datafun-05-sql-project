# Import from Python Standard Library first
import sqlite3
import pathlib
import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define paths using joinpath
db_file_path = pathlib.Path("financial_data.db")
sql_file_path = pathlib.Path("sql").joinpath("spy_data.sql")
spy_data_path = pathlib.Path("data").joinpath("spy_data.csv")

# Ensure the SQL folder exists and create SQL script for table creation
def verify_and_create_folders(paths):
    """Verify and create folders if they don't exist."""
    for path in paths:
        folder = path.parent
        if not folder.exists():
            print(f"Creating folder: {folder}")
            folder.mkdir(parents=True, exist_ok=True)
        else:
            print(f"Folder already exists: {folder}")

def create_database(db_path):
    """Create a new SQLite database file if it doesn't exist."""
    try:
        conn = sqlite3.connect(db_path)
        conn.close()
        print("Database created successfully.")
    except sqlite3.Error as e:
        print(f"Error creating the database: {e}")

def create_tables(db_path):
    """Create tables in the database."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS spy_data (
        date TEXT PRIMARY KEY,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER
    );
    """
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(create_table_sql)
            print("Table 'spy_data' created successfully.")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")

def fetch_spy_data():
    """Fetch SPY data from Alpha Vantage API."""
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "SPY",  # S&P 500 ETF
        "apikey": api_key,
        "outputsize": "full"  # Use "compact" for the last 100 data points
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching SPY data: {response.status_code}")
        return None

def insert_data_into_db(db_path, spy_data):
    """Insert fetched SPY data into the SQLite database."""
    if spy_data:
        time_series = spy_data.get("Time Series (Daily)", {})
        try:
            with sqlite3.connect(db_path) as conn:
                for date, data in time_series.items():
                    conn.execute("""
                        INSERT OR REPLACE INTO spy_data (date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (date, float(data['1. open']), float(data['2. high']),
                          float(data['3. low']), float(data['4. close']),
                          int(data['5. volume'])))
                print("SPY data inserted successfully.")
        except sqlite3.Error as e:
            print(f"Error inserting SPY data: {e}")

def main():
    paths_to_verify = [sql_file_path, spy_data_path]
    verify_and_create_folders(paths_to_verify)   

    create_database(db_file_path)
    create_tables(db_file_path)

    spy_data = fetch_spy_data()
    insert_data_into_db(db_file_path, spy_data)

if __name__ == "__main__":
    main()
