from kline_scapping_all_pairs_monthly import BinanceVisionKlinesMultiDownloader
from dotenv import load_dotenv
from premium_index_srapping_all_pairs_monthly import BinanceVisionPremiumIndexMultiDownloader
from typing import Literal, List
import os
import pymongo
import logging


class DownloadManager:
    def __init__(self, data_kind: Literal['klines', 'premium_index_klines'], symbols: list, interval: str = '1m',
                 start_date: str = None, end_date: str = None,
                 symbol_batch: int = 1,
                 batch_size: int = 20, max_concurrent_symbols: int = 5,
                 first_download: bool = False
                 ):
        self.symbols = symbols
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size = batch_size
        self.max_concurrent_symbols = max_concurrent_symbols
        self.data_kind = data_kind
        self.arg_dict = self._generate_args_dict_by_data_kind()
        self.symbol_batch = symbol_batch
        self.first_download = first_download
        load_dotenv()

    def _generate_args_dict_by_data_kind(self):
        if self.data_kind == 'klines':
            return {'database': BinanceVisionKlinesMultiDownloader.DB_NAME, 'collection_str': 'klines'}
        elif self.data_kind == 'premium_index_klines':
            return {'database': BinanceVisionPremiumIndexMultiDownloader.DB_NAME,
                    'collection_str': 'premium_index_klines'}

    def _generate_mongo_uri(self):
        return f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}/{self.arg_dict['database']}?tls=true&authSource=admin&replicaSet=db-mongodb-sgp1-43703"

    def get_all_collections(self):
        client = pymongo.MongoClient(self._generate_mongo_uri())
        db = client.get_database()
        return db.list_collection_names()

    def get_collection_name(self, symbol: str) -> str:
        return f"{symbol.lower()}_{self.interval}_{self.arg_dict['collection_str']}"

    def if_collections_exist(self, symbols: List[str]) -> List[bool]:
        collection_names = [self.get_collection_name(symbol) for symbol in symbols]
        all_collections = self.get_all_collections()
        return [collection_name in all_collections for collection_name in collection_names]

    def if_in_collection_metadata(self, symbols: List[str]) -> List[bool]:
        collection_names = [self.get_collection_name(symbol) for symbol in symbols]
        client = pymongo.MongoClient(self._generate_mongo_uri())
        db = client[self.arg_dict['database']]
        metadata_collection = db["collection_metadata"]

        found_collections = metadata_collection.find(
            {"collection_name": {"$in": collection_names}},
            {"collection_name": 1, "_id": 0}  # Only return the collection_name field
        )
        found_set = {doc["collection_name"] for doc in found_collections}

        # Check each name against the set
        client.close()
        return [name in found_set for name in collection_names]

    async def download(self):
        for i in range(0, len(self.symbols), self.symbol_batch):
            batch_symbols = self.symbols[i:i + self.symbol_batch]
            downloader = BinanceVisionKlinesMultiDownloader(batch_symbols, start_date=self.start_date,
                                                            end_date=self.end_date,
                                                            interval=self.interval, batch_size=self.batch_size, max_concurrent_symbols=self.max_concurrent_symbols,
                                                            first_download=self.first_download)

            try:
                results = await downloader.download_all()

                # 打印結果摘要
                print("\n===== Download Summary =====")
                for result in results:
                    symbol = result["symbol"]
                    print(f"\nSymbol: {symbol}")
                    print(f"Total records: {result['total_records']}")
                    print(f"Date range: {result['min_datetime']} to {result['max_datetime']}")

                    # 顯示元數據
                    metadata = await downloader.get_collection_metadata(symbol)
                    print(f"Records in DB: {metadata['record_count']}")
                    print(f"Date range in DB: {metadata['date_range']}")

            except Exception as e:
                logging.error(f"Error downloading data: {e}")
            finally:
                # 關閉 MongoDB 連接
                downloader.mongo_client.close()

