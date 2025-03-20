import akshare as ak
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from tqdm import tqdm


class Resource:
    def __init__(self):
        self.downloadPaths = {'Stock': 'StockData'}
        self.stocks = {}
        self.frequency = 'daily'
        self.startDate = '20000101'
        self.endDate = datetime.now().strftime("%Y%m%d")
        self.adjust = 'qfq'
        self.MAX_WORKERS = os.cpu_count()

    def exists(self, path):
        return os.path.exists(path)

    def checkExists(self, path):
        if not self.exists(path):
            self.makeDirs(path)

    def makeDirs(self, path):
        os.makedirs(path)

    def saveStock(self, stock_code, df):
        path = f'downloads/{self.downloadPaths['Stock']}'
        filename = f'{stock_code}_{self.stocks[stock_code]}'
        self.checkExists(path)
        df.to_csv(os.path.join(path, filename))


class StockData:
    def __init__(self, resource):
        self.Resource = resource

    def get_all_stock(self):
        try:
            df = ak.stock_info_a_code_name()
            stock_dict = dict(zip(df["code"], df["name"]))
            return stock_dict
        except:
            return {}

    def get_stock_data(self, stock_code):
        try:
            df = ak.stock_zh_a_hist(symbol=stock_code,
                                    period=self.Resource.frequency,
                                    start_date=self.Resource.startDate,
                                    end_date=self.Resource.endDate,
                                    adjust=self.Resource.adjust)

            df['日期'] = pd.to_datetime(df['日期'])
            df.set_index('日期', inplace=True)

            return df
        except:
            return pd.DataFrame()

    def get_stock_data_from_file_single(self, stock_code, day='2022-09-14'):
        path = f'downloads/{self.Resource.downloadPaths['Stock']}'
        filename = f'{stock_code}_{self.Resource.stocks[stock_code]}'
        df = pd.read_csv(os.path.join(path, filename), index_col=0, parse_dates=True)
        data = df.loc[day] if day in df.index else None
        return data

    def get_stock_data_from_file(self, stock_code):
        path = f'downloads/{self.Resource.downloadPaths['Stock']}'
        filename = f'{stock_code}_{self.Resource.stocks[stock_code]}'
        df = pd.read_csv(os.path.join(path, filename), index_col=0, parse_dates=True)
        return df

    def get_stock_data_single(self, stock_code, day='2022-09-14'):
        df = self.get_stock_data(stock_code)
        data = df.loc[day] if day in df.index else None
        return data

    def check_stock(self, stock_code, df: pd.DataFrame):
            if df.empty:
                return False
            stock_name: str = self.Resource.stocks[stock_code]
            if stock_code.startswith('688') or stock_code.startswith('8') or stock_code.startswith(
                    '9') or 'ST' in stock_name:
                return False

            return True


class StockDownloader:
    def __init__(self, resource, stockData):
        self.Resource = resource
        self.stockData = stockData

    def download_single_stock(self, stock_code):
        df = self.stockData.get_stock_data(stock_code)
        if self.stockData.check_stock(stock_code, df):
            self.Resource.saveStock(stock_code, df)

    def download_all_stock(self):
        with ThreadPoolExecutor(max_workers=self.Resource.MAX_WORKERS) as executor:
            future_to_stock = {executor.submit(self.download_single_stock, stock_code): stock_code for stock_code in
                               self.Resource.stocks}

            with tqdm(total=len(future_to_stock), desc="DOWNLOAD PROGRESS", unit="stock(s)") as pbar:
                for future in as_completed(future_to_stock):
                    pbar.update(1)  # 每完成一个任务，进度条 +1

class StockAnalyzer:
    def __init__(self, resource, stockData):
        self.Resource = resource
        self.stockData = stockData



class StockSystem:
    def __init__(self):
        self.Resource = Resource()
        self.StockData = StockData(self.Resource)
        self.StockDownloader = StockDownloader(self.Resource, self.StockData)
        self.init()

    def init(self):
        self.Resource.stocks = self.StockData.get_all_stock()

if __name__ == "__main__":
    stockSystem = StockSystem()
    OUT = print
    IN = input
