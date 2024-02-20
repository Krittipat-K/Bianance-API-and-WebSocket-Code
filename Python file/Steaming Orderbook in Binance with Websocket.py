import asyncio
import websockets
import aiofiles  # Library for asynchronous file I/O
import json
import os
import datetime
import csv

async def orderbook_Binance_download(pair: str, market: str) -> None:
    '''This Python function downloads order book data from Binance for a specified trading pair and market
    type, and saves it to CSV files with updates every day.
    
    Parameters
    ----------
    pair : str
        The `pair` parameter in the `orderbook_Binance_download` function is a string that represents the
        trading pair for which you want to download the order book data. It typically consists of two
        assets, for example, "BTCUSDT" where BTC is the base asset and USDT is the quote asset.
    market : str
        The `market` parameter in the `orderbook_Binance_download` function is used to specify the type of
        market for which you want to download the order book data. It is an integer value that determines
        the websocket URL to connect for fetching the order book updates. In the provided code snippet, the
        values are mapped to specific websocket URLs and market names.
    '''

    # Convert the trading pair to lowercase
    pair_lower = pair.lower()

    # Get the current working directory
    current_path = os.getcwd()
    
    # Define websocket URLs and market names based on market type
    market_type = {
        1: f'wss://stream.binance.com:9443/ws/{pair_lower}@depth@100ms',
        2: f'wss://fstream.binance.com/ws/{pair_lower}@depth@100ms',
    }
    market_name = {
        1: 'Spot',
        2: 'USD-F',
    }

    # Select the websocket URL based on the provided market parameter
    websocket_url = market_type[market]

    # Get the current date and calculate Unix timestamps for today and tomorrow
    today = datetime.datetime.now().date()
    next_day = datetime.datetime.now() + datetime.timedelta(days=1)
    next_day = next_day.date()
    UNIX_today = int(datetime.datetime(today.year, today.month, today.day).timestamp() * 1000)
    UNIX_next_day = int(datetime.datetime(next_day.year, next_day.month, next_day.day).timestamp() * 1000)

    # Create directory to store raw data if it doesn't exist
    if not os.path.exists(f'{current_path}/Raw Data/{pair.upper()}'):
        os.makedirs(f'{current_path}/Raw Data/{pair.upper()}')

    # Define field names for the CSV file
    fieldnames = ["E", "b", "a"]

    # Infinite loop for continuously receiving order book updates
    while True:
        try:
            # Establish websocket connection
            async with websockets.connect(websocket_url) as websocket:
                while True:
                    # Receive data from websocket
                    data = await websocket.recv()

                    # Parse received data as JSON and extract relevant fields into a dictionary
                    data_dict = {key: json.loads(data)[key] for key in ["E", "b", "a"]}

                    # Update Unix timestamps if necessary
                    if UNIX_next_day <= data_dict['E']:
                        today = datetime.datetime.now().date()
                        next_day = datetime.datetime.now() + datetime.timedelta(days=1)
                        next_day = next_day.date()
                        UNIX_today = int(datetime.datetime(today.year, today.month, today.day).timestamp() * 1000)
                        UNIX_next_day = int(datetime.datetime(next_day.year, next_day.month, next_day.day).timestamp() * 1000)

                    # Calculate relative time
                    data_dict['E'] = int(data_dict['E'] - UNIX_today)

                    # Generate filename for the CSV file
                    filename = f'Orderbook_{market_name[market]}_{pair.upper()}_updates_{today}.csv'

                    # Create CSV file if it does not exist and write header
                    if not os.path.exists(f'{current_path}/Raw Data/{pair.upper()}/{filename}'):
                        async with aiofiles.open(f'{current_path}/Raw Data/{pair.upper()}/{filename}', mode='a', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            await writer.writeheader() 

                    # Open CSV file and write data
                    async with aiofiles.open(f'{current_path}/Raw Data/{pair.upper()}/{filename}', mode='a', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        await writer.writerow(data_dict)
                
        # Catch any exceptions and write them to an error log file
        except Exception as e:
            async with aiofiles.open(f'{current_path}/Raw Data/{pair.upper()}_Error/{market_name[market]}_Error.text', mode='a') as f:
                await f.write(f'{e} at {datetime.datetime.now()}' + '\n')
if __name__ == "__main__":
    pair = input("Input pair : ")
    market = int(input("Input market(number) \n1 Spot\n2 USD F\n:"))
    asyncio.run(orderbook_Binance_download(pair,market))