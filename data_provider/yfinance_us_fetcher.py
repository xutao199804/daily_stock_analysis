# -*- coding: utf-8 -*-
"""
===================================
YfinanceUSFetcher - 美股数据源 (Priority 1)
===================================

数据来源：Yahoo Finance（通过 yfinance 库）
特点：美股实时数据、高质量、免费
定位：美股分析的主力数据源

关键特性：
1. 直接使用美股代码（AAPL, MSFT, TSLA 等）
2. 支持实时行情和历史数据
3. 自动处理股票分割和分红调整
4. 提供基本面数据（PE、市值等）
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS

logger = logging.getLogger(__name__)


class YfinanceUSFetcher(BaseFetcher):
    """
    Yahoo Finance 美股数据源实现
    
    优先级：1（最高，美股主力数据源）
    数据来源：Yahoo Finance
    
    支持的股票代码格式：
    - 美股：AAPL, MSFT, TSLA, GOOGL 等
    - ETF：SPY, QQQ, IWM 等
    - 指数：^GSPC (标普500), ^IXIC (纳斯达克), ^DJI (道琼斯)
    
    关键特性：
    - 实时行情（15分钟延迟）
    - 历史数据（完整复权）
    - 基本面数据
    - 高可靠性
    """
    
    name = "YfinanceUSFetcher"
    priority = 1  # 美股场景下设为最高优先级
    
    def __init__(self):
        """初始化 YfinanceUSFetcher"""
        pass
    
    def _validate_stock_code(self, stock_code: str) -> str:
        """
        验证并标准化美股代码
        
        美股代码格式：
        - 普通股票：AAPL, MSFT（大写字母）
        - 指数：^GSPC, ^IXIC（以 ^ 开头）
        - ETF：SPY, QQQ
        
        Args:
            stock_code: 原始代码
            
        Returns:
            标准化后的代码（大写）
        """
        code = stock_code.strip().upper()
        
        # 美股代码通常是1-5个字母，或以^开头的指数
        if code.startswith('^') or (len(code) >= 1 and len(code) <= 5 and code.isalpha()):
            return code
        
        logger.warning(f"股票代码 {code} 格式可能不正确，但仍尝试获取")
        return code
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 Yahoo Finance 获取美股原始数据
        
        流程：
        1. 验证股票代码
        2. 调用 yfinance API
        3. 处理返回数据
        """
        import yfinance as yf
        
        # 验证并标准化代码
        yf_code = self._validate_stock_code(stock_code)
        
        logger.debug(f"调用 yfinance.download({yf_code}, {start_date}, {end_date})")
        
        try:
            # 使用 yfinance 下载数据
            df = yf.download(
                tickers=yf_code,
                start=start_date,
                end=end_date,
                progress=False,  # 禁止进度条
                auto_adjust=True,  # 自动调整价格（复权）
            )
            
            if df.empty:
                raise DataFetchError(f"Yahoo Finance 未查询到 {stock_code} 的数据")
            
            return df
            
        except Exception as e:
            if isinstance(e, DataFetchError):
                raise
            raise DataFetchError(f"Yahoo Finance 获取数据失败: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化 Yahoo Finance 数据
        
        yfinance 返回的列名：
        Open, High, Low, Close, Volume（索引是日期）
        
        需要映射到标准列名：
        date, open, high, low, close, volume, amount, pct_chg
        """
        df = df.copy()
        
        # 重置索引，将日期从索引变为列
        df = df.reset_index()
        
        # 列名映射（yfinance 使用首字母大写）
        column_mapping = {
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
        }
        
        df = df.rename(columns=column_mapping)
        
        # 计算涨跌幅
        if 'close' in df.columns:
            df['pct_chg'] = df['close'].pct_change() * 100
            df['pct_chg'] = df['pct_chg'].fillna(0).round(2)
        
        # 计算成交额（美股单位：美元）
        # 成交额 = 成交量 * 收盘价（近似值）
        if 'volume' in df.columns and 'close' in df.columns:
            df['amount'] = (df['volume'] * df['close']).round(2)
        else:
            df['amount'] = 0
        
        # 添加股票代码列
        df['code'] = stock_code.upper()
        
        # 只保留需要的列
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票基本信息（美股特有功能）
        
        返回信息包括：
        - 公司名称
        - 市值
        - PE 比率
        - 52周最高/最低
        - 等等
        
        Args:
            stock_code: 股票代码
            
        Returns:
            股票信息字典，失败返回 None
        """
        import yfinance as yf
        
        try:
            yf_code = self._validate_stock_code(stock_code)
            ticker = yf.Ticker(yf_code)
            info = ticker.info
            
            # 提取关键信息
            return {
                'symbol': info.get('symbol', stock_code),
                'name': info.get('longName', info.get('shortName', 'N/A')),
                'market_cap': info.get('marketCap', 0),
                'pe_ratio': info.get('trailingPE', 0),
                'forward_pe': info.get('forwardPE', 0),
                'pb_ratio': info.get('priceToBook', 0),
                'dividend_yield': info.get('dividendYield', 0),
                'fifty_two_week_high': info.get('fiftyTwoWeekHigh', 0),
                'fifty_two_week_low': info.get('fiftyTwoWeekLow', 0),
                'sector': info.get('sector', 'N/A'),
                'industry': info.get('industry', 'N/A'),
            }
        except Exception as e:
            logger.warning(f"获取 {stock_code} 基本信息失败: {e}")
            return None


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = YfinanceUSFetcher()
    
    # 测试美股数据获取
    test_stocks = ['AAPL', 'MSFT', 'TSLA']
    
    for stock in test_stocks:
        try:
            print(f"\n{'='*50}")
            print(f"测试股票: {stock}")
            print(f"{'='*50}")
            
            # 获取历史数据
            df = fetcher.get_daily_data(stock)
            print(f"✅ 获取成功，共 {len(df)} 条数据")
            print("\n最近5天数据：")
            print(df.tail())
            
            # 获取基本信息
            info = fetcher.get_stock_info(stock)
            if info:
                print(f"\n📊 基本信息：")
                print(f"  公司名称: {info['name']}")
                print(f"  市值: ${info['market_cap']:,.0f}")
                print(f"  PE比率: {info['pe_ratio']:.2f}")
                print(f"  行业: {info['sector']} - {info['industry']}")
            
        except Exception as e:
            print(f"❌ 获取失败: {e}")
