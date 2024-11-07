import yfinance as yf
from tqdm import tqdm
from ei.utils import Config
from ei.repos import DummyStockRepository
# Define the stock ticker





def get_all_ticker(config:Config):
    with open(config.historical_data_scraper.ticker_file) as f:
        return set(f.read().splitlines())

def download(ticker):
    return yf.download(ticker, period='max')

def fill_database(repo,config):
    avalible_tickers = get_all_ticker(config)
    downloaded_tickers = set(repo.get_all())
    ticker_to_download = avalible_tickers - downloaded_tickers
    for ticker in tqdm(ticker_to_download, desc="Downloading Stock Data"):
        data = download(ticker)
        repo.insert_data(data)

def update_database(repo):
    for ticker in tqdm(repo.get_keys(), desc="Updating Stock Data"):
        data = download(ticker)
        repo.update(ticker, data)

if __name__ == "__main__":
    from ei.utils import load_config
    config = load_config()
    repo = DummyStockRepository("DEBUG",config)
    fill_database(repo,config)
