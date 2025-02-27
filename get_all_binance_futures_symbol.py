#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import argparse
import pandas as pd
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import os
import sys

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BinanceFuturesSymbolFetcher:
    """
    幣安期貨交易對獲取工具
    """

    def __init__(self, use_testnet: bool = False):
        """
        初始化

        Args:
            use_testnet: 是否使用測試網絡，默認為 False
        """
        self.use_testnet = use_testnet

        # API 端點
        if use_testnet:
            self.base_url = "https://testnet.binancefuture.com"
        else:
            self.base_url = "https://fapi.binance.com"  # USDT 永續合約

        self.session = requests.Session()
        # 設置請求頭，模擬瀏覽器請求
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        })

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Dict:
        """
        執行 API 請求並處理可能的錯誤

        Args:
            url: API 請求 URL
            params: 請求參數字典

        Returns:
            Dict: API 響應
        """
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()  # 如果狀態碼不是 200，拋出異常
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API 請求失敗: {e}")
            raise

    def get_usdt_futures_exchange_info(self) -> Dict:
        """
        獲取 USDT 永續合約交易所信息

        Returns:
            Dict: 交易所信息
        """
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        return self._make_request(url)

    def get_usdt_futures_24h_tickers(self) -> Dict:
        """
        獲取所有 USDT 永續合約 24 小時價格統計

        Returns:
            List[Dict]: 所有交易對的 24 小時價格統計
        """
        url = f"{self.base_url}/fapi/v1/ticker/24hr"
        return self._make_request(url)

    def get_usdm_symbols(self, include_details: bool = False) -> List:
        """
        獲取所有 USDM 期貨交易對

        Args:
            include_details: 是否包含交易對詳細信息，
                           True 返回完整信息，False 僅返回交易對名稱

        Returns:
            List: 交易對列表或交易對詳細信息列表
        """
        try:
            # 獲取交易所信息
            exchange_info = self.get_usdt_futures_exchange_info()

            if not include_details:
                # 僅返回交易對名稱列表
                symbols = [
                    symbol['symbol']
                    for symbol in exchange_info.get('symbols', [])
                    if symbol.get('status') == 'TRADING'
                ]
                logger.info(f"找到 {len(symbols)} 個 USDM 期貨交易對")
                return symbols

            # 獲取價格統計數據
            tickers = {item['symbol']: item for item in self.get_usdt_futures_24h_tickers()}

            # 返回帶詳細信息的交易對列表
            symbol_details = []
            for symbol_info in exchange_info.get('symbols', []):
                if symbol_info.get('status') == 'TRADING':
                    symbol = symbol_info['symbol']
                    ticker_info = tickers.get(symbol, {})

                    symbol_data = {
                        'symbol': symbol,
                        'baseAsset': symbol_info.get('baseAsset', ''),
                        'quoteAsset': symbol_info.get('quoteAsset', ''),
                        'volume_24h': float(ticker_info.get('volume', 0)),
                        'quoteVolume_24h': float(ticker_info.get('quoteVolume', 0)),
                        'priceChange_24h': float(ticker_info.get('priceChangePercent', 0)),
                        'lastPrice': float(ticker_info.get('lastPrice', 0))
                    }

                    symbol_details.append(symbol_data)

            logger.info(f"找到 {len(symbol_details)} 個 USDM 期貨交易對（含詳細信息）")
            return symbol_details

        except Exception as e:
            logger.error(f"獲取 USDM 期貨交易對時出錯: {e}")
            return []

    def export_to_json(self, data: List, filename: str) -> None:
        """
        導出數據為 JSON 格式

        Args:
            data: 要導出的數據
            filename: 文件名
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        logger.info(f"已導出數據到 {filename}")

    def export_to_csv(self, data: List[Dict], filename: str) -> None:
        """
        導出數據為 CSV 格式

        Args:
            data: 要導出的數據（必須是字典列表）
            filename: 文件名
        """
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        logger.info(f"已導出數據到 {filename}")


def main():
    """主函數"""
    parser = argparse.ArgumentParser(description='獲取幣安 USDM 期貨交易對列表')

    parser.add_argument('--testnet', action='store_true', help='使用測試網絡')
    parser.add_argument('--details', action='store_true', help='包含詳細信息')
    parser.add_argument('--export', action='store_true', help='是否導出數據')
    parser.add_argument('--format', choices=['json', 'csv'], default='json', help='導出格式')
    parser.add_argument('--output-dir', type=str, default='.', help='輸出目錄')

    args = parser.parse_args()

    # 創建輸出目錄（如果需要）
    if args.export:
        os.makedirs(args.output_dir, exist_ok=True)

    # 獲取當前日期
    date_str = datetime.now().strftime('%Y%m%d')

    # 初始化獲取器
    fetcher = BinanceFuturesSymbolFetcher(use_testnet=args.testnet)

    try:
        # 獲取 USDM 交易對
        logger.info("獲取幣安 USDM 期貨交易對...")
        symbols = fetcher.get_usdm_symbols(include_details=args.details)

        # 打印結果
        if args.details:
            print(f"找到 {len(symbols)} 個 USDM 期貨交易對:")
            # 以表格形式打印部分信息
            for symbol_info in symbols[:10]:  # 只打印前 10 個交易對
                print(
                    f"{symbol_info['symbol']} - 24h成交量: {symbol_info['volume_24h']}, 價格: {symbol_info['lastPrice']}")
            if len(symbols) > 10:
                print(f"... 另外還有 {len(symbols) - 10} 個交易對")
        else:
            print(f"找到 {len(symbols)} 個 USDM 期貨交易對:")
            for symbol in symbols:
                print(symbol)

        # 如果指定了導出選項，導出數據
        if args.export:
            if args.format == 'json':
                filename = os.path.join(args.output_dir, f"binance_usdm_symbols_{date_str}.json")
                fetcher.export_to_json(symbols, filename)
            elif args.format == 'csv' and args.details:
                filename = os.path.join(args.output_dir, f"binance_usdm_symbols_{date_str}.csv")
                fetcher.export_to_csv(symbols, filename)
            elif args.format == 'csv':
                # 轉換簡單列表為字典列表以便導出為 CSV
                symbols_dicts = [{'symbol': s} for s in symbols]
                filename = os.path.join(args.output_dir, f"binance_usdm_symbols_{date_str}.csv")
                fetcher.export_to_csv(symbols_dicts, filename)

        # 返回交易對列表
        return symbols

    except Exception as e:
        logger.error(f"程序執行出錯: {e}", exc_info=True)
        return []


if __name__ == "__main__":
    symbols_list = main()
    # 可以將 symbols_list 作為結果使用
    sys.exit(0 if symbols_list else 1)