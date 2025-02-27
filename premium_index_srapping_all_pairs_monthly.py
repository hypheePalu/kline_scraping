import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Any, Tuple
import zipfile
import io
from tqdm import tqdm as tqdm_sync
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
import hashlib


class BinanceVisionPremiumIndexMultiDownloader:
    DB_NAME = 'binance_future_premium_index_klines'
    def __init__(self, symbols: List[str], start_date: str, end_date: str,
                 interval: str = "1m", batch_size: int = 20, max_concurrent_symbols: int = 5,
                 first_download: bool = False):
        self.symbols = [s.upper() for s in symbols]
        self.base_url = "https://data.binance.vision/data/futures/um/monthly/premiumIndexKlines"
        self.interval = interval
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.batch_size = batch_size
        self.max_concurrent_symbols = max_concurrent_symbols
        self.semaphore = asyncio.Semaphore(15)  # Limit concurrent requests to Binance API
        self.first_download = first_download
        # Setup MongoDB
        load_dotenv()
        mongo_uri = f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}/{self.DB_NAME}?tls=true&authSource=admin&replicaSet=db-mongodb-sgp1-43703"
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client[self.DB_NAME]
        self.metadata_collection = self.db["collection_metadata"]
        self.checksum_collection = self.db["download_checksums"]  # New: Collection for storing file checksums

        # Setup logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    async def setup_timeseries_collection(self, symbol: str):
        """Set up time series collection"""
        collection = f"{symbol.lower()}_{self.interval}_premium_index_klines"
        collections = await self.db.list_collection_names()

        if collection not in collections:
            # Create time series collection
            await self.db.create_collection(collection,
                                            timeseries={
                                                'timeField': 'open_time',
                                                'metaField': 'metadata',
                                                'granularity': 'seconds'
                                            })
            self.logger.info(f"Created timeseries collection: {collection}")
        return collection

    async def check_if_collection_exists(self, collection_name: str) -> bool:
        """Efficiently check if a collection exists using a filter

        Args:
            collection_name: Collection name to check

        Returns:
            bool: True if the collection exists, False otherwise
        """
        cursor = await self.db.list_collections(filter={"name": collection_name})
        return cursor.to_list(length=1) != []

    async def remove_duplicates_from_collection(self, collection: str):
        """Remove duplicate records from a time series collection"""
        self.logger.info(f"Cleaning duplicate records in time series collection {collection}...")

        # Use aggregation pipeline to find duplicate records
        pipeline = [
            {"$group": {
                "_id": {"open_time": "$open_time", "symbol": "$metadata.symbol"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ]

        # Check if there are any duplicate records
        duplicates = await self.db[collection].aggregate(pipeline).to_list(length=None)

        if not duplicates:
            self.logger.info(f"No duplicate records found in {collection}")
            return 0

        # Calculate time range of duplicate records
        duplicate_open_times = [dup["_id"]["open_time"] for dup in duplicates]

        if not duplicate_open_times:
            self.logger.info(f"No duplicate records that need processing found in {collection}")
            return 0

        min_time = min(duplicate_open_times)
        max_time = max(duplicate_open_times)

        self.logger.info(f"Found duplicates in time range: {min_time} to {max_time}")

        # For time series collections, we need a special strategy:
        # 1. Find all records in the duplicate time range
        # 2. Select non-duplicate records from them
        # 3. Delete all records in the original time range
        # 4. Re-insert the non-duplicate records

        # Create temporary regular collection name, ensure uniqueness
        import time
        temp_collection = f"temp_{collection}_{int(time.time())}"

        try:
            # Create a temporary regular collection (non-time series)
            await self.db.create_collection(temp_collection)
            self.logger.info(f"Created temporary collection: {temp_collection}")

            # Extract all records in the time range to the temporary collection
            # For each unique (open_time, symbol) combination, only keep the first record
            cursor = self.db[collection].find({
                "open_time": {"$gte": min_time, "$lte": max_time}
            })

            # Use a dictionary to track unique records
            unique_records = {}
            async for doc in cursor:
                # Create unique key
                key = (doc["open_time"], doc["metadata"]["symbol"])
                if key not in unique_records:
                    unique_records[key] = doc

            # Save unique records to temporary collection
            if unique_records:
                await self.db[temp_collection].insert_many(list(unique_records.values()))

            # Count records in temporary collection
            unique_count = await self.db[temp_collection].count_documents({})
            self.logger.info(f"Saved {unique_count} unique records in the temporary collection")

            # Count total records in original time range
            original_count = await self.db[collection].count_documents({
                "open_time": {"$gte": min_time, "$lte": max_time}
            })
            self.logger.info(f"Original collection has {original_count} records in the time range")

            # Calculate number of duplicate records to be removed
            duplicates_to_remove = original_count - unique_count
            self.logger.info(f"Will remove {duplicates_to_remove} duplicate records")

            if duplicates_to_remove <= 0:
                self.logger.info("No duplicate records to delete, ending early")
                return 0

            # Delete all records in original date range
            delete_result = await self.db[collection].delete_many({
                "open_time": {"$gte": min_time, "$lte": max_time}
            })
            self.logger.info(f"Deleted {delete_result.deleted_count} records from original time series collection")

            # Restore non-duplicate records from temporary collection to time series collection
            # For time series collections, we need to insert in batches
            batch_size = 1000  # Smaller batch size is recommended for time series collections
            all_docs = await self.db[temp_collection].find({}).to_list(length=None)

            # Insert in batches back to original collection
            for i in range(0, len(all_docs), batch_size):
                batch = all_docs[i:i + batch_size]

                # Remove _id field to let MongoDB generate new _id
                for doc in batch:
                    if "_id" in doc:
                        del doc["_id"]

                await self.db[collection].insert_many(batch)
                self.logger.info(
                    f"Restored {len(batch)} records to time series collection (batch {i // batch_size + 1})")

            self.logger.info(f"Restored {len(all_docs)} unique records to time series collection")

            return duplicates_to_remove

        except Exception as e:
            self.logger.error(f"Error cleaning duplicate records in time series collection: {e}")
            raise

        finally:
            # Always clean up temporary collection
            try:
                if await self.check_if_collection_exists(temp_collection):
                    await self.db.drop_collection(temp_collection)
                    self.logger.info(f"Deleted temporary collection {temp_collection}")
            except Exception as e:
                self.logger.error(f"Error deleting temporary collection: {e}")

    async def get_collection_timestamps(self, collection: str) -> Dict[str, datetime]:
        """Get earliest and latest timestamps in the collection"""
        min_pipeline = [{"$group": {"_id": None, "min_timestamp": {"$min": "$open_time"}}}]
        max_pipeline = [{"$group": {"_id": None, "max_timestamp": {"$max": "$open_time"}}}]

        min_result = await self.db[collection].aggregate(min_pipeline).to_list(length=1)
        max_result = await self.db[collection].aggregate(max_pipeline).to_list(length=1)

        min_timestamp = min_result[0]["min_timestamp"] if min_result else None
        max_timestamp = max_result[0]["max_timestamp"] if max_result else None

        return {"min": min_timestamp, "max": max_timestamp}

    async def update_metadata(self, symbol: str, total_records: int, min_timestamp: datetime, max_timestamp: datetime):
        """Update metadata for the collection"""
        collection = f"{symbol.lower()}_{self.interval}_premium_index_klines"

        # Get latest record count
        count_of_collection = await self.db[collection].count_documents({})

        # Get current time range
        timestamps = await self.get_collection_timestamps(collection)
        original_min = timestamps["min"]
        original_max = timestamps["max"]

        # Calculate final time range
        final_min = min(min_timestamp, original_min) if original_min else min_timestamp
        final_max = max(max_timestamp, original_max) if original_max else max_timestamp

        metadata = {
            "collection_name": collection,
            "symbol": symbol,
            "interval": self.interval,
            "record_count": count_of_collection,
            "start_time": final_min,
            "end_time": final_max,
            "date_range": f"{final_min.strftime('%Y-%m-%d')} to {final_max.strftime('%Y-%m-%d')}",
            "last_updated": datetime.now(),
            "last_update_date_range": f"{self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}"
        }

        await self.metadata_collection.update_one(
            {"collection_name": collection},
            {"$set": metadata},
            upsert=True
        )
        self.logger.info(f"Updated metadata for {collection}")

    def _get_dates_list(self) -> List[datetime]:
        """Generate a list of dates from start_date to end_date"""
        dates = []
        current_date = self.start_date
        while current_date <= self.end_date:
            dates.append(current_date)
            current_date += relativedelta(months=1)
        return dates

    def _get_file_url(self, symbol: str, date: datetime) -> str:
        """Generate file URL for specific date and trading pair"""
        date_str = date.strftime("%Y-%m")
        filename = f"{symbol}-{self.interval}-{date_str}.zip"
        return f"{self.base_url}/{symbol}/{self.interval}/{filename}"

    def _get_checksum_url(self, symbol: str, date: datetime) -> str:
        """Generate checksum file URL for specific date and trading pair"""
        date_str = date.strftime("%Y-%m")
        filename = f"{symbol}-{self.interval}-{date_str}.zip.CHECKSUM"
        return f"{self.base_url}/{symbol}/{self.interval}/{filename}"

    async def _download_checksum(self, url: str) -> Optional[str]:
        """Download checksum file and return its content"""
        async with self.semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 404:
                            self.logger.debug(f"Checksum file not found: {url}")
                            return None

                        if response.status != 200:
                            self.logger.error(f"Error downloading checksum {url}: {response.status}")
                            return None

                        content = await response.text()
                        # Extract the SHA256 hash from the content
                        # Handle different formats:
                        # 1. "SHA256(filename)= hash"
                        # 2. Just the hash value
                        # 3. "hash  filename" (standard checksum format)
                        content = content.strip()
                        try:
                            if "=" in content:
                                # Format: SHA256(filename)= hash
                                return content.split("=")[1].strip()
                            elif len(content) == 64:
                                # Just a raw hash (64 characters for SHA256)
                                return content
                            elif ' ' in content and len(content.split()[0]) == 64:
                                # Format: "hash filename" - common in standard checksum files
                                return content.split()[0]
                            else:
                                self.logger.warning(f"Unusual checksum format: {content}, extracting first 64 chars")
                                # As a fallback, try to extract first 64 characters if they look like a hash
                                if len(content) >= 64:
                                    potential_hash = content[:64]
                                    if all(c in '0123456789abcdefABCDEF' for c in potential_hash):
                                        return potential_hash
                                return content
                        except Exception as e:
                            self.logger.error(f"Invalid checksum format: {content}")
                            return None

            except Exception as e:
                self.logger.error(f"Error processing checksum {url}: {e}")
                return None

    def _compute_file_hash(self, file_content: bytes) -> str:
        """Compute SHA256 hash of file content"""
        return hashlib.sha256(file_content).hexdigest()

    async def _verify_checksum(self, file_url: str, file_content: bytes) -> bool:
        """Verify file content against its checksum"""
        # Get checksum URL by replacing .zip with .zip.CHECKSUM
        checksum_url = file_url + ".CHECKSUM"

        # Download checksum
        expected_hash_full = await self._download_checksum(checksum_url)
        if not expected_hash_full:
            self.logger.warning(f"No checksum available for {file_url}, skipping verification")
            return True  # If no checksum is available, consider it valid

        # Extract just the hash part (first 64 characters) if it contains more
        expected_hash = expected_hash_full.split()[0] if ' ' in expected_hash_full else expected_hash_full

        # Compute hash of downloaded content
        actual_hash = self._compute_file_hash(file_content)

        # Compare hashes
        is_valid = expected_hash.lower() == actual_hash.lower()

        if not is_valid:
            self.logger.error(f"Checksum verification failed for {file_url}")
            self.logger.error(f"Expected: {expected_hash}")
            self.logger.error(f"Actual: {actual_hash}")
        else:
            self.logger.debug(f"Checksum verified for {file_url}")

        # Store checksum result in database
        await self._store_checksum_result(file_url, expected_hash_full, actual_hash, is_valid)

        return is_valid

    async def _store_checksum_result(self, file_url: str, expected_hash: str,
                                     actual_hash: str, is_valid: bool) -> None:
        """Store checksum verification result in database"""
        checksum_data = {
            "file_url": file_url,
            "expected_hash": expected_hash,
            "actual_hash": actual_hash,
            "is_valid": is_valid,
            "verification_time": datetime.now()
        }

        await self.checksum_collection.update_one(
            {"file_url": file_url},
            {"$set": checksum_data},
            upsert=True
        )

    async def _download_file(self, symbol: str, url: str) -> Optional[List[Dict[str, Any]]]:
        """Download and process a single file with checksum verification"""
        async with self.semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 404:
                            self.logger.debug(f"File not found: {url}")
                            return None

                        if response.status != 200:
                            self.logger.error(f"Error downloading {url}: {response.status}")
                            return None

                        content = await response.read()

                        # Verify checksum before processing
                        is_valid = await self._verify_checksum(url, content)
                        if not is_valid:
                            self.logger.error(f"Skipping {url} due to checksum verification failure")
                            return None

                        # Unzip content
                        with zipfile.ZipFile(io.BytesIO(content)) as zf:
                            csv_filename = zf.namelist()[0]
                            with zf.open(csv_filename) as csv_file:
                                # Read CSV content
                                df = pd.read_csv(csv_file, header=None, skiprows=1, names=[
                                    'open_time', 'open', 'high', 'low', 'close',
                                    'volume', 'close_time', 'quote_volume', 'count',
                                    'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
                                ])
                                df = df[['open_time', 'open', 'high', 'low', 'close', 'close_time']]

                                df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
                                df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
                                df['metadata'] = df.apply(
                                    lambda row: {
                                        'symbol': symbol,
                                        'interval': self.interval
                                    },
                                    axis=1
                                )

                                return df.to_dict(orient='records')

            except Exception as e:
                self.logger.error(f"Error processing {url}: {e}")
                return None

    async def process_symbol_batch(self, symbol: str, dates: List[datetime], pbar: tqdm_sync) -> Dict[str, Any]:
        """Process a batch of dates for a single trading pair"""
        total_records = 0
        min_datetime = None
        max_datetime = None
        collection = await self.setup_timeseries_collection(symbol)

        # Create download tasks
        download_tasks = []
        for date in dates:
            url = self._get_file_url(symbol, date)
            task = asyncio.create_task(self._download_file(symbol, url))
            download_tasks.append((date, task))

        # Process completed tasks
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
                        await self.db[collection].insert_many(records, ordered=False)
                        total_records += len(records)
                        self.logger.debug(f"Saved {len(records)} records for {symbol} on {date}")
                    except Exception as e:
                        if "E11000 duplicate key error" in str(e):
                            self.logger.debug(f"Found duplicates for {symbol} on {date}")
                        else:
                            self.logger.error(f"Error saving to MongoDB: {e}")
            except Exception as e:
                self.logger.error(f"Error processing {symbol} on {date}: {e}")
            finally:
                pbar.update(1)

        deleted_count = 0
        # Clean up duplicate records
        if not self.first_download:
            deleted_count = await self.remove_duplicates_from_collection(collection)

        # Return results
        return {
            "symbol": symbol,
            "collection": collection,
            "records": total_records,
            "min_datetime": min_datetime,
            "max_datetime": max_datetime,
            "duplicates": deleted_count
        }

    async def process_symbol(self, symbol: str) -> Dict[str, Any]:
        """Process all data for a single trading pair"""
        self.logger.info(f"Starting download for {symbol}")
        all_dates = self._get_dates_list()
        total_dates = len(all_dates)

        # Create progress bar
        pbar = tqdm_sync(
            total=total_dates,
            desc=f"{symbol} Progress",
            position=0,
            leave=True
        )

        total_records = 0
        min_timestamp = None
        max_timestamp = None

        # Process in batches
        for i in range(0, total_dates, self.batch_size):
            batch_dates = all_dates[i:i + self.batch_size]
            batch_result = await self.process_symbol_batch(symbol, batch_dates, pbar)

            total_records += batch_result["records"]

            if batch_result["min_datetime"]:
                if min_timestamp is None or batch_result["min_datetime"] < min_timestamp:
                    min_timestamp = batch_result["min_datetime"]

            if batch_result["max_datetime"]:
                if max_timestamp is None or batch_result["max_datetime"] > max_timestamp:
                    max_timestamp = batch_result["max_datetime"]

            # Add delay between batches
            if i + self.batch_size < total_dates:
                await asyncio.sleep(0.01)

        pbar.close()

        # Update metadata
        if min_timestamp and max_timestamp:
            await self.update_metadata(symbol, total_records, min_timestamp, max_timestamp)

        return {
            "symbol": symbol,
            "total_records": total_records,
            "min_datetime": min_timestamp,
            "max_datetime": max_timestamp
        }

    async def download_all(self) -> Tuple[Any]:
        """Download data for all trading pairs, using semaphore to limit concurrency"""
        # Create semaphore to limit concurrent processing of trading pairs
        symbol_semaphore = asyncio.Semaphore(self.max_concurrent_symbols)

        async def process_with_semaphore(symbol):
            async with symbol_semaphore:
                return await self.process_symbol(symbol)

        # Create tasks for each trading pair
        tasks = [process_with_semaphore(symbol) for symbol in self.symbols]

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        return results

    async def get_collection_metadata(self, symbol: str):
        """Get metadata for a specific collection"""
        collection = f"{symbol.lower()}_{self.interval}_premium_index_klines"
        return await self.metadata_collection.find_one({"collection_name": collection})

    async def list_all_collections_metadata(self):
        """List metadata for all collections"""
        return await self.metadata_collection.find().to_list(length=100)

    async def get_checksum_verification_report(self, symbol: str = None,
                                               start_date: datetime = None,
                                               end_date: datetime = None) -> List[Dict]:
        """Get a report of checksum verification results, optionally filtered by symbol and date range"""
        filter_query = {}

        if symbol:
            # Create a partial match for the file URL containing the symbol
            filter_query["file_url"] = {"$regex": f"/{symbol}/", "$options": "i"}

        if start_date or end_date:
            time_query = {}
            if start_date:
                time_query["$gte"] = start_date
            if end_date:
                time_query["$lte"] = end_date

            if time_query:
                filter_query["verification_time"] = time_query

        return await self.checksum_collection.find(filter_query).to_list(length=None)


async def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Multi-symbol download example
    symbols = ["BTCUSDT"]
    downloader = BinanceVisionPremiumIndexMultiDownloader(
        symbols=symbols,
        start_date="2024-01-01",
        end_date="2024-03-02",
        interval="1m",
        batch_size=50,
        max_concurrent_symbols=5  # Process at most 5 symbols concurrently
    )

    try:
        results = await downloader.download_all()

        # Print result summary
        print("\n===== Download Summary =====")
        for result in results:
            symbol = result["symbol"]
            print(f"\nSymbol: {symbol}")
            print(f"  Total records: {result['total_records']}")
            print(f"  Date range: {result['min_datetime']} to {result['max_datetime']}")

            # Display metadata
            metadata = await downloader.get_collection_metadata(symbol)
            print(f"  Records in DB: {metadata['record_count']}")
            print(f"  Date range in DB: {metadata['date_range']}")

        # Get checksum verification report
        checksum_report = await downloader.get_checksum_verification_report()
        valid_count = sum(1 for item in checksum_report if item["is_valid"])
        invalid_count = len(checksum_report) - valid_count

        print("\n===== Checksum Verification Summary =====")
        print(f"Total files verified: {len(checksum_report)}")
        print(f"Valid files: {valid_count}")
        print(f"Invalid files: {invalid_count}")

        if invalid_count > 0:
            print("\nInvalid files details:")
            for item in checksum_report:
                if not item["is_valid"]:
                    print(f"  File: {item['file_url']}")
                    print(f"  Expected hash: {item['expected_hash']}")
                    print(f"  Actual hash: {item['actual_hash']}")
                    print(f"  Verification time: {item['verification_time']}")
                    print()

    except Exception as e:
        logging.error(f"Error downloading data: {e}")
    finally:
        # Close MongoDB connection
        downloader.mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())