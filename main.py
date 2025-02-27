from get_all_binance_futures_symbol import BinanceFuturesSymbolFetcher
from kline_scapping_all_pairs_monthly import BinanceVisionKlinesMultiDownloader
import logging
import asyncio
import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def get_all_collections():
    mongo_uri = f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}/{os.getenv('MONGO_NAME')}?tls=true&authSource=admin&replicaSet=db-mongodb-sgp1-43703"
    client = pymongo.MongoClient(mongo_uri)
    db = client["binance_future"]
    return db.list_collection_names()


def get_collection_name(symbol: str, interval: str = '1m') -> str:
    return f"{symbol.lower()}_{interval}_klines"


def if_collection_exist(symbol: str, interval: str = '1m') -> bool:
    collection_name = get_collection_name(symbol, interval)
    return collection_name in get_all_collections()


def if_in_collection_metadata(symbol: str, interval: str = '1m') -> bool:
    collection_name = get_collection_name(symbol, interval)
    mongo_uri = f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}/{os.getenv('MONGO_NAME')}?tls=true&authSource=admin&replicaSet=db-mongodb-sgp1-43703"
    client = pymongo.MongoClient(mongo_uri)
    db = client["binance_future"]
    metadata_collection = db["collection_metadata"]
    result = metadata_collection.find_one({"collection_name": collection_name})

    # Close the MongoDB connection
    client.close()

    # If result is not None, the collection_name exists
    return result is not None


async def main():
    symbol_list = BinanceFuturesSymbolFetcher().get_usdm_symbols()
    symbol_batch = 1

    for i in range(0, len(symbol_list), symbol_batch):
        batch_symbols = symbol_list[i:i + symbol_batch]
        if if_in_collection_metadata(batch_symbols[0]):
            print(f"Collection {get_collection_name(batch_symbols[0])} already exists")
            continue

        if if_collection_exist(batch_symbols[0]):
            print('not fully downloaded')
            mode = False
        else:
            mode = True

        downloader = BinanceVisionKlinesMultiDownloader(batch_symbols, start_date='2020-01-01', end_date='2024-12-31',
                                                        interval="1m", batch_size=5, max_concurrent_symbols=5,
                                                        first_download=mode)

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


if __name__ == "__main__":
    # asyncio.run(main())
    print(BinanceFuturesSymbolFetcher().get_usdm_symbols())