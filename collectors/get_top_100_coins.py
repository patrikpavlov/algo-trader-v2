import requests

def get_top_100_symbols():
    """
    Fetches the top 100 cryptocurrencies by market cap from the CoinGecko API
    and returns a list of their symbols.
    """
    # The API endpoint for fetching market data
    url = "https://api.coingecko.com/api/v3/coins/markets"
    
    # Parameters for the API request
    params = {
        'vs_currency': 'usd',           # Compare against USD
        'order': 'market_cap_desc',     # Order by market cap descending
        'per_page': 100,                # Get 100 results
        'page': 1,                      # Get the first page
        'sparkline': 'false'
    }

    try:
        # Make the API request
        response = requests.get(url, params=params)
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Extract the 'symbol' from each coin dictionary
        symbols = [coin['symbol'] for coin in data]
        
        return symbols

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from CoinGecko API: {e}")
        return None

if __name__ == "__main__":
    top_symbols = get_top_100_symbols()
    if top_symbols:
        print("Top 100 Crypto Symbols by Market Cap:")
        print(top_symbols)
        binance_symbols = [f"{symbol}usdt" for symbol in top_symbols if symbol != 'usdt']
        print("\nFormatted for Binance (e.g., btcusdt):")
        print(binance_symbols) 
        