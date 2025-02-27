import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Optional
import zipfile
import io
from pymongo.operations import ReplaceOne, UpdateMany
from tqdm import tqdm as tqdm_sync
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv


class BinanceVisionKlineDownloader:
    def __init__(self, symbol: str, start_date: str, end_date: str,
                 interval: str = "1m", batch_size: int = 20):
        self.base_url = "https://data.binance.vision/data/futures/um/daily/klines"
        self.symbol = symbol.upper()
        self.interval = interval
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.batch_size = batch_size
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(15)

        # Setup MongoDB
        load_dotenv()
        mongo_uri = f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}/{os.getenv('MONGO_NAME')}?tls=true&authSource=admin&replicaSet=db-mongodb-sgp1-43703"
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client[os.getenv('MONGO_NAME')]
        self.collection = f"{symbol.lower()}_{interval}_klines"
        self.metadata_collection = self.db["collection_metadata"]
        # Setup logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    async def setup_timeseries_collection(self):
        """設置時間序列集合和索引"""
        collections = await self.db.list_collection_names()

        if self.collection not in collections:
            # 創建時間序列集合
            await self.db.create_collection(self.collection,
                                            timeseries={
                                                'timeField': 'open_time',  # 時間欄位
                                                'metaField': 'metadata',  # 元數據欄位
                                                'granularity': 'seconds'  # 粒度
                                            }
                                            )

    async def remove_duplicates_from_collection(self):
        """刪除整個集合中的重複時間戳記錄，只保留每個時間戳的第一條記錄"""
        self.logger.info(f"cleaning duplicated records in {self.collection}...")

        # 使用聚合管道找出並刪除重複記錄
        pipeline = [
            # 按時間戳和交易對分組
            {"$group": {
                "_id": {"open_time": "$open_time", "symbol": "$metadata.symbol"},
                "docs": {"$push": "$$ROOT"},
                "count": {"$sum": 1}
            }},
            # 只保留有多個記錄的組
            {"$match": {"count": {"$gt": 1}}},
            # 展開記錄
            {"$unwind": {"path": "$docs", "includeArrayIndex": "index"}},
            # 只保留重複的記錄（排除第一個記錄）
            {"$match": {"index": {"$gt": 0}}},
            # 取得重複記錄的 _id
            {"$project": {"_id": "$docs._id"}}
        ]

        # 收集所有重複記錄的 ID
        duplicate_ids = []
        async for doc in self.db[self.collection].aggregate(pipeline):
            duplicate_ids.append(doc["_id"])

        if duplicate_ids:
            # 分批刪除重複記錄，避免單次操作太大
            batch_size = 1000
            total_deleted = 0

            for i in range(0, len(duplicate_ids), batch_size):
                batch_ids = duplicate_ids[i:i + batch_size]
                delete_result = await self.db[self.collection].delete_many({"_id": {"$in": batch_ids}})
                total_deleted += delete_result.deleted_count
                self.logger.info(
                    f"deleted {i // batch_size + 1}/{(len(duplicate_ids) + batch_size - 1) // batch_size}: {delete_result.deleted_count} records")

            self.logger.info(f"finish cleaning：deleted {total_deleted} dulicated records")
        else:
            self.logger.info("no duplicated records found")

        return {"deleted_count": len(duplicate_ids)}

    async def get_min_timestamp(self):
        """獲取集合中最小的 timestamp"""
        pipeline = [
            {"$group": {"_id": None, "min_timestamp": {"$min": "$open_time"}}}
        ]

        result = await self.db[self.collection].aggregate(pipeline).to_list(length=1)

        if result:
            return result[0]["min_timestamp"]
        else:
            return None

    async def get_max_timestamp(self):
        """獲取集合中最小的 timestamp"""
        pipeline = [
            {"$group": {"_id": None, "max_timestamp": {"$max": "$open_time"}}}
        ]

        result = await self.db[self.collection].aggregate(pipeline).to_list(length=1)

        if result:
            return result[0]["max_timestamp"]
        else:
            return None

    async def update_metadata(self, total_records: int, min_timestamp: datetime, max_timestamp: datetime):
        """Update metadata for the collection"""
        # original_metadata = await self.metadata_collection.find_one({"collection_name": self.collection})
        count_of_collection = await self.db[self.collection].count_documents({})
        original_min_timestamp = await self.get_min_timestamp()
        original_max_timestamp = await self.get_max_timestamp()

        min_timestamp = min(min_timestamp, original_min_timestamp) if original_min_timestamp else min_timestamp
        max_timestamp = max(max_timestamp, original_max_timestamp) if original_max_timestamp else max_timestamp
        metadata = {
            'date_range': f"{min_timestamp.strftime('%Y-%m-%d')} to {max_timestamp.strftime('%Y-%m-%d')}",
            "collection_name": self.collection,
            "symbol": self.symbol,
            'record_count': count_of_collection,
            "interval": self.interval,
            "start_time": min_timestamp,
            "end_time": max_timestamp,
            "last_updated": datetime.now(),
            "last_update_date_range": f"{self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}"
        }

        await self.metadata_collection.update_one(
            {"collection_name": self.collection},
            {"$set": metadata},
            upsert=True
        )

    def _get_dates_list(self) -> List[datetime]:
        """Generate list of dates between start and end date"""
        dates = []
        current_date = self.start_date
        while current_date <= self.end_date:
            dates.append(current_date)
            current_date += timedelta(days=1)
        return dates

    def _get_file_url(self, date: datetime) -> str:
        """Generate URL for specific date"""
        date_str = date.strftime("%Y-%m-%d")
        filename = f"{self.symbol}-{self.interval}-{date_str}.zip"
        return f"{self.base_url}/{self.symbol}/{self.interval}/{filename}"

    async def _download_file(self, url: str, pbar: Optional[tqdm_sync] = None) -> Optional[List[dict]]:
        """Download and process single file"""
        async with self.semaphore:
            try:
                async with self.session.get(url) as response:
                    if response.status == 404:
                        self.logger.warning(f"File not found: {url}")
                        if pbar:
                            pbar.update(1)
                        return None

                    if response.status != 200:
                        self.logger.error(f"Error downloading {url}: {response.status}")
                        if pbar:
                            pbar.update(1)
                        return None

                    content = await response.read()

                    # Extract ZIP content
                    with zipfile.ZipFile(io.BytesIO(content)) as zf:
                        csv_filename = zf.namelist()[0]
                        with zf.open(csv_filename) as csv_file:
                            # Read CSV content
                            df = pd.read_csv(csv_file, header=None, skiprows=1, names=[
                                'open_time', 'open', 'high', 'low', 'close',
                                'volume', 'close_time', 'quote_volume', 'count',
                                'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
                            ])

                            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
                            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
                            df['metadata'] = df.apply(
                                lambda row: {
                                    'symbol': self.symbol,
                                    'interval': self.interval
                                },
                                axis=1
                            )

                            records = df.to_dict(orient='records')
                            # Convert to MongoDB documents
                            if pbar:
                                pbar.update(1)
                            return records

            except Exception as e:
                self.logger.error(f"Error processing {url}: {e}")
                if pbar:
                    pbar.update(1)
                return None

    async def _process_batch(self, dates: List[datetime], overall_pbar: tqdm_sync) -> dict:
        """Download and save a batch of files with parallel downloading"""
        min_datetime = None
        max_datetime = None
        total_records = 0

        # 創建一個隊列來收集下載結果
        download_tasks = []

        # 啟動所有下載任務
        for date in dates:
            url = self._get_file_url(date)
            task = asyncio.create_task(self._download_file(url, None))  # 不傳遞進度條
            download_tasks.append((date, task))

        # 創建批次進度條
        batch_pbar = tqdm_sync(
            total=len(dates),
            desc=f"Batch {dates[0].strftime('%Y-%m-%d')}",
            leave=False
        )

        # 處理完成的任務
        for date, task in download_tasks:
            try:
                records = await task

                if records:
                    batch_timestamps = [record['open_time'] for record in records]
                    batch_min = min(batch_timestamps)
                    batch_max = max(batch_timestamps)

                    if min_datetime is None or batch_min < min_datetime:
                        min_datetime = batch_min

                    if max_datetime is None or batch_max > max_datetime:
                        max_datetime = batch_max

                    try:
                        # 保存到 MongoDB
                        await self.db[self.collection].insert_many(records, ordered=False)
                        total_records += len(records)
                        self.logger.debug(f"Saved {len(records)} records for {date}")

                    except Exception as e:
                        if "E11000 duplicate key error" in str(e):
                            # 處理重複鍵錯誤
                            self.logger.warning(f"Found duplicates while inserting records for {date}")
                        else:
                            self.logger.error(f"Error saving to MongoDB: {e}")

            except Exception as e:
                self.logger.error(f"Error downloading data for {date}: {e}")

            finally:
                batch_pbar.update(1)
                overall_pbar.update(1)

        batch_pbar.close()

        # 清理重複記錄
        deleted_count = await self.remove_duplicates_from_collection()

        return {
            "records": total_records,
            "min_datetime": min_datetime,
            "max_datetime": max_datetime,
            "duplicates": deleted_count
        }

    async def download_and_save(self) -> dict:
        """Main method to download and save data"""
        await self.setup_timeseries_collection()
        all_dates = self._get_dates_list()
        total_records = 0
        min_timestamp = None
        max_timestamp = None

        # Create overall progress bar
        total_files = len(all_dates)
        overall_pbar = tqdm_sync(
            total=total_files,
            desc="Overall Progress",
            position=0
        )

        # Create aiohttp session
        async with aiohttp.ClientSession() as session:
            self.session = session

            # Process in batches
            for i in range(0, len(all_dates), self.batch_size):
                batch_dates = all_dates[i:i + self.batch_size]

                batch_result = await self._process_batch(batch_dates, overall_pbar)
                total_records += batch_result['records']

                if batch_result["min_datetime"]:
                    if min_timestamp is None or batch_result["min_datetime"] < min_timestamp:
                        min_timestamp = batch_result["min_datetime"]

                if batch_result["max_datetime"]:
                    if max_timestamp is None or batch_result["max_datetime"] > max_timestamp:
                        max_timestamp = batch_result["max_datetime"]

                # Add delay between batches
                if i + self.batch_size < len(all_dates):
                    await asyncio.sleep(2.7)

        overall_pbar.close()
        if min_timestamp and max_timestamp:
            await self.update_metadata(total_records, min_timestamp, max_timestamp)

        return {
            "total_records": total_records,
            "min_datetime": min_timestamp,
            "max_datetime": max_timestamp,
            'deleted_count': batch_result['duplicates']
        }

    async def get_collection_metadata(self):
        """Get metadata for the collection"""
        return await self.metadata_collection.find_one({"collection_name": self.collection})

    async def list_all_collections_metadata(self):
        """List metadata for all collections"""
        return await self.metadata_collection.find().to_list(length=100)


async def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example usage
    downloader = BinanceVisionKlineDownloader(
        symbol="SUIUSDT",
        start_date="2020-01-01",
        end_date="2020-01-02",
        interval="1m",
        batch_size=1000
    )

    try:
        result = await downloader.download_and_save()
        print(f"\nDownloaded and saved {result['total_records']} records")
        print(f"Date range: {result['min_datetime']} to {result['max_datetime']}")
        print(f"Deleted {result['deleted_count']} duplicated records")

        # Display metadata
        metadata = await downloader.get_collection_metadata()
        print("\nCollection Metadata:")
        for key, value in metadata.items():
            print(f"  {key}: {value}")

    except Exception as e:
        logging.error(f"Error downloading data: {e}")
    finally:
        # Close MongoDB connection
        downloader.mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
