import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from the environment variable
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

# Define the base URL for Alpha Vantage
base_url = "https://www.alphavantage.co/query"

# Function to fetch SPY data
def fetch_spy_data():
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

# Fetch SPY data
spy_data = fetch_spy_data()

# Write the data to a CSV file
if spy_data:
    time_series = spy_data.get("Time Series (Daily)", {})
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(time_series, orient='index')
    df.columns = ["Open", "High", "Low", "Close", "Volume"]
    df.index.name = 'Date'
    df = df.rename(columns={
        "Open": "1. open",
        "High": "2. high",
        "Low": "3. low",
        "Close": "4. close",
        "Volume": "5. volume"
    })
    df.to_csv('spy_data.csv', index=True)

    print("SPY data has been written to spy_data.csv.")
