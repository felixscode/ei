from abc import ABC, abstractmethod
from ei.utils import get_logger,Config
import h5py
import polars as pl
from pathlib import Path

class AbstractStockRepository(ABC):
    def __init__(self,log_level,config):
        self.logger = get_logger(__name__, log_level)
        self.config = config

    @abstractmethod
    def insert_data(self, data):
        pass

    @abstractmethod
    def get_by_key(self, key):
        pass

    @abstractmethod
    def get_all(self):
        pass
    
    @abstractmethod
    def get_keys(self):
        pass
    
    @abstractmethod
    def update(self,key,data):
        pass


class DummyStockRepository(AbstractStockRepository):
    def __init__(self,log_level,config):
        super().__init__(log_level,config)
        self.logger.warning("Using Dummy Repository")

    def insert_data(self, data):
        self.logger.warning("Inserting data into Dummy Repository")

    def get_by_key(self, key):
        self.logger.warning("Getting data from Dummy Repository")
    
    def get_all(self):
        self.logger.warning("Getting all data from Dummy Repository")
        return []

    def get_keys(self):
        self.logger.warning("Getting keys from Dummy Repository")
        return []
    
    def update(self,key,data):
        self.logger.warning("Updating data in Dummy Repository")
        pass



class HDF5StockRepository(AbstractStockRepository):
    def __init__(self, log_level, config:Config):
        super().__init__(log_level, config)
        self.hdf5_file = config.local_repository.directory

        # Ensure HDF5 file exists
        hdf5_path = Path(self.hdf5_file)
        hdf5_path.touch(exist_ok=True)
        self.db = None

    def insert_data(self, data):
        """
        Insert stock data into the HDF5 file.
        Assumes `data` is a Polars DataFrame.
        """
        
        with h5py.File(self.hdf5_file, 'a') as f:
            ticker = data[0, 'ticker']  # Assume 'ticker' column exists in the Polars DataFrame

            # If the ticker group doesn't exist, create it
            if ticker not in f:
                f.create_group(ticker)
            
            # Convert Polars DataFrame to numpy arrays for HDF5 storage
            dates = data['date'].to_numpy()
            opens = data['open'].to_numpy()
            closes = data['close'].to_numpy()
            volumes = data['volume'].to_numpy()

            # Create a dataset for the specific ticker with new data
            if ticker in f:
                ticker_group = f[ticker]
                ticker_group.create_dataset('date', data=dates)
                ticker_group.create_dataset('open', data=opens)
                ticker_group.create_dataset('close', data=closes)
                ticker_group.create_dataset('volume', data=volumes)

            self.logger.info(f"Inserted data for ticker: {ticker}")


    def get_by_key(self, key):
        """
        Retrieve stock data by ticker.
        """
        try:
            with h5py.File(self.hdf5_file, 'r') as f:
                if key in f:
                    ticker_group = f[key]
                    dates = ticker_group['date'][:]
                    opens = ticker_group['open'][:]
                    closes = ticker_group['close'][:]
                    volumes = ticker_group['volume'][:]

                    # Convert to Polars DataFrame
                    df = pl.DataFrame({
                        'date': dates,
                        'open': opens,
                        'close': closes,
                        'volume': volumes
                    })
                    return df
                else:
                    self.logger.warning(f"Ticker {key} not found.")
                    return None
        except Exception as e:
            self.logger.error(f"Error retrieving data by key {key}: {e}")
            return None

    def get_all(self):
        """
        Retrieve all stock data.
        """
        try:
            all_data = {}
            with h5py.File(self.hdf5_file, 'r') as f:
                for ticker in f:
                    ticker_group = f[ticker]
                    dates = ticker_group['date'][:]
                    opens = ticker_group['open'][:]
                    closes = ticker_group['close'][:]
                    volumes = ticker_group['volume'][:]

                    df = pl.DataFrame({
                        'date': dates,
                        'open': opens,
                        'close': closes,
                        'volume': volumes
                    })

                    all_data[ticker] = df

            return all_data
        except Exception as e:
            self.logger.error(f"Error retrieving all data: {e}")
            return {}

    def get_keys(self):
        """
        Retrieve all keys (tickers).
        """
        try:
            with h5py.File(self.hdf5_file, 'r') as f:
                return list(f.keys())
        except Exception as e:
            self.logger.error(f"Error retrieving keys: {e}")
            return []

    def update(self, key, data):
        """
        Update stock data for a specific ticker.
        """
        try:
            with h5py.File(self.hdf5_file, 'a') as f:
                if key in f:
                    ticker_group = f[key]

                    # Ensure the data is in Polars DataFrame format
                    dates = data['date'].to_numpy()
                    opens = data['open'].to_numpy()
                    closes = data['close'].to_numpy()
                    volumes = data['volume'].to_numpy()

                    # Replace the datasets or update with new data
                    ticker_group['date'][:] = dates
                    ticker_group['open'][:] = opens
                    ticker_group['close'][:] = closes
                    ticker_group['volume'][:] = volumes

                    self.logger.info(f"Updated data for ticker: {key}")
                else:
                    self.logger.warning(f"Ticker {key} not found to update.")
        except Exception as e:
            self.logger.error(f"Error updating data for {key}: {e}")
