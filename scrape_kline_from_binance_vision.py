import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Optional
import zipfile
import io
from tqdm.asyncio import tqdm_asyncio  # Changed import name
from tqdm import tqdm as tqdm_sync
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv


class BinanceVisionDownloader:
    def __init__(self, symbol: str, start_date: str, end_date: str,
                 interval: str = "1m", batch_size: int = 20):
        self.base_url = "https://data.binance.vision/data/futures/um/daily/klines"
        self.symbol = symbol.upper()
        self.interval = interval
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.batch_size = batch_size
        self.session: Optional[aiohttp.ClientSession] = None

        # Setup MongoDB
        load_dotenv()
        mongo_uri = f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}/{os.getenv('MONGO_NAME')}?tls=true&authSource=admin&replicaSet=db-mongodb-sgp1-43703"
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client[os.getenv('MONGO_NAME')]
        self.collection = f"{symbol.lower()}_{interval}_klines"

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

    async def _download_file(self, url: str, date: datetime, pbar: Optional[tqdm_sync] = None) -> Optional[List[dict]]:
        """Download and process single file"""
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

    async def _process_batch(self, dates: List[datetime], overall_pbar: tqdm_sync) -> int:
        """Download and save a batch of files"""
        total_records = 0

        # Create a regular batch progress bar, not an async context manager
        batch_pbar = tqdm_sync(
            total=len(dates),
            desc=f"Batch {dates[0].strftime('%Y-%m-%d')}",
            leave=False
        )

        for date in dates:
            url = self._get_file_url(date)
            records = await self._download_file(url, date, batch_pbar)

            if records:
                try:

                    result = await self.db[self.collection].insert_many(records)
                    total_records += len(records)
                    self.logger.debug(f"Saved {len(records)} records for {date}")

                except Exception as e:
                    self.logger.error(f"Error saving to MongoDB: {e}")

            overall_pbar.update(1)

        batch_pbar.close()
        return total_records

    async def download_and_save(self) -> int:
        """Main method to download and save data"""
        await self.setup_timeseries_collection()
        all_dates = self._get_dates_list()
        total_records = 0

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

                batch_records = await self._process_batch(batch_dates, overall_pbar)
                total_records += batch_records

                # Add delay between batches
                if i + self.batch_size < len(all_dates):
                    await asyncio.sleep(1)

        overall_pbar.close()
        return total_records


async def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Example usage
    downloader = BinanceVisionDownloader(
        symbol="BTCUSDT",
        start_date="2024-01-01",
        end_date="2024-01-10",
        interval="1m",
        batch_size=10  # Reduced batch size for demonstration
    )

    try:
        total_records = await downloader.download_and_save()
        print(f"\nDownloaded and saved {total_records} records")

    except Exception as e:
        logging.error(f"Error downloading data: {e}")
    finally:
        # Close MongoDB connection
        downloader.mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
