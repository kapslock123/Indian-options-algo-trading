from datetime import datetime
import pandas as pd
from datetime import timedelta
import requests
import json
from constants.eod_Interval import EODIntervalEnum
from constants.chart_exchange import ChartExchangeEnum
from constants.asset_type import AssetTypeEnum
from constants.intraday_interval import IntradayIntervalEnum
from APIConnect.APIConnect import APIConnect

# Function to get the last Thursday of the month (Bank Nifty Monthly Expiry)
def get_last_thursday(year, month):
    # Start from the last day of the month and move backward until we find a Thursday
    last_day = pd.to_datetime(f'{year}-{month}-01') + pd.offsets.MonthEnd(1)
    while last_day.weekday() != 3:  # Thursday is weekday 3
        last_day -= timedelta(days=1)
    return last_day.date()

# Function to get the next Friday (Sensex Weekly Expiry)


def get_next_friday(current_date):
    # Calculate days until next Friday
    days_ahead = 4 - current_date.weekday()
    if days_ahead <= 0:  # If today is Friday or later, go to next week's Friday
        days_ahead += 7
    return current_date + timedelta(days=days_ahead)


def get_historical_data(api_connect_instance, interval, exchange, asset_type, symbol, start, end):
    # Parameters for the API request
    interval = interval
    exc = exchange
    aTyp = asset_type
    symbol = symbol

    base_url = "https://nc.nuvamawealth.com/edelmw-content/content"
    url = base_url + f"/charts/v2/main/{interval}/{exc}/{aTyp}/{symbol}"

    api_constants = api_connect_instance._APIConnect__constants

    all_data_frames = []  # List to store data batches
    ltt = end  # Initialize `ltt` to the end date

    while True:
        # Set up the payload with the current ltt (last timestamp)
        data = {
            'chTyp': "Interval",
            'conti': False,
            # Format the last timestamp as needed
            'ltt': ltt.strftime('%Y-%m-%d')
        }

        # Make the API request
        response = requests.post(url, headers={
            "Authorization": api_constants.JSessionId,
            "Source": api_constants.ApiKey,
            "SourceToken": api_constants.VendorSession,
            "AppIdKey": api_constants.AppIdKey,
            "Content-type": "application/json"
        }, data=json.dumps(data)) 

        # Extract data
        data = response.json().get("data")
        if not data:
            print(f"No data returned from API for the date {ltt}")
            break

        # Create DataFrame for current batch
        ohlc_dict = {
            "open": data["pltPnts"]["open"],
            "high": data["pltPnts"]["high"],
            "low": data["pltPnts"]["low"],
            "close": data["pltPnts"]["close"],
            "time": data["pltPnts"]["ltt"]
        }
        df = pd.DataFrame(ohlc_dict)
        df["time"] = pd.to_datetime(df["time"])
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)

        # Collect the DataFrame in the list
        all_data_frames.append(df)

        # Update `ltt` to the earliest timestamp in this batch
        first_timestamp = df.index[0]

        # Check if we've reached the start date
        if first_timestamp <= start:
            break  # Stop if the start date is covered

        # Update `ltt` for the next API call
        ltt = first_timestamp

    # Concatenate all DataFrames once at the end
    full_data = pd.concat(all_data_frames)
    full_data = full_data[full_data.index >= start]  # Filter for start date
    full_data.sort_index(inplace=True)  # Ensure final DataFrame is sorted

    return full_data

def get_historical_data_closest_timestamp(api_connect_instance, interval, exchange, asset_type, symbol, timestamp, delta):
    # Parameters for the API request
    interval = interval
    exc = exchange
    aTyp = asset_type
    symbol = symbol

    base_url = "https://nc.nuvamawealth.com/edelmw-content/content"
    url = base_url + f"/charts/v2/main/{interval}/{exc}/{aTyp}/{symbol}"

    api_constants = api_connect_instance._APIConnect__constants

    all_data_frames = []  # List to store data batches
    # Get atleast 1 day of data after the timestamp to be safe that we get the data
    tdelta = timedelta(days=1)
    ltt = timestamp  + tdelta # Initialize `ltt` to the end date


    # Set up the payload with the current ltt (last timestamp)
    data = {
        'chTyp': "Interval",
        'conti': False,
        # Format the last timestamp as needed
        'ltt': ltt.strftime('%Y-%m-%d')
    }

    # Make the API request
    response = requests.post(url, headers={
        "Authorization": api_constants.JSessionId,
        "Source": api_constants.ApiKey,
        "SourceToken": api_constants.VendorSession,
        "AppIdKey": api_constants.AppIdKey,
        "Content-type": "application/json"
    }, data=json.dumps(data)) 

    # Extract data
    data = response.json().get("data")
    if not data:
        print(f"No data returned from API for the date {ltt}")
     

    # Create DataFrame for current batch
    ohlc_dict = {
        "open": data["pltPnts"]["open"],
        "high": data["pltPnts"]["high"],
        "low": data["pltPnts"]["low"],
        "close": data["pltPnts"]["close"],
        "time": data["pltPnts"]["ltt"]
    }
    df = pd.DataFrame(ohlc_dict)
    df["time"] = pd.to_datetime(df["time"])
    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)

    # Lookup the closest timestamp to the requested timestamp
    lookup_data = df[(df.index > timestamp - delta) & (df.index <= timestamp)]

    if lookup_data.empty:
        print(f"No data found for the timestamp {timestamp}")
        return None

    return lookup_data.iloc[-1]



def get_option_chain_for_strike_price(api_connect_instance, exchange, symbol, strike_price, expiry_date):
    # Parameters for the API request
    exc = exchange
    symbol = symbol
    strike = strike_price
    expiry = expiry_date

    base_url = "https://nc.nuvamawealth.com/edelmw-content/content"
    url = base_url + f"/charts/v2/optionChain/{exc}/{symbol}/{strike}/{expiry}"

    api_constants = api_connect_instance._APIConnect__constants

    # Make the API request
    response = requests.get(url, headers={
        "Authorization": api_constants.JSessionId,
        "Source": api_constants.ApiKey,
        "SourceToken": api_constants.VendorSession,
        "AppIdKey": api_constants.AppIdKey
    })

    # Extract data
    data = response.json().get("data")
    if not data:
        print(f"No data returned from API for the strike price {strike_price}")
        return None

    return data