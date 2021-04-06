import logging
import os
import pickle
from datetime import datetime, time

import yfinance as yf

from lumibot import LUMIBOT_YAHOO_CACHE_PATH

from .helpers import get_lumibot_datetime


class YahooHelper:

    # =========Internal initialization parameters and methods============

    CACHING_ENABLED = False

    if not os.path.exists(LUMIBOT_YAHOO_CACHE_PATH):
        try:
            os.makedirs(LUMIBOT_YAHOO_CACHE_PATH)
            CACHING_ENABLED = True
        except Exception as e:
            logging.critical(
                f"""Could not create cache folder for Yahoo data because of the following error:
                {e}. Please fix the issue to use data caching."""
            )
    else:
        CACHING_ENABLED = True

    # ====================Caching methods=================================

    @staticmethod
    def is_up_to_date(last_update):
        this_date = get_lumibot_datetime()

        # Last date changed
        if this_date.date() > last_update.date():
            return False

        # Before 1AM
        # 1h margin given to Yahoo from midnight
        # to update its data
        ref = time(1, 0)
        if last_update.time() >= ref:
            return True

        return False

    @staticmethod
    def check_pickle_file(symbol):
        if YahooHelper.CACHING_ENABLED:
            pickle_file_path = os.path.join(
                LUMIBOT_YAHOO_CACHE_PATH, f"{symbol}.pickle"
            )
            if os.path.exists(pickle_file_path):
                with open(pickle_file_path, "rb") as f:
                    data = pickle.load(f)
                    last_update = data["last_update"]
                    if YahooHelper.is_up_to_date(last_update):
                        return data
        return None

    @staticmethod
    def dump_pickle_file(symbol, data):
        if YahooHelper.CACHING_ENABLED:
            pickle_file_path = os.path.join(
                LUMIBOT_YAHOO_CACHE_PATH, f"{symbol}.pickle"
            )
            if not os.path.exists(pickle_file_path):
                with open(pickle_file_path, "wb") as f:
                    pickle.dump(data, f)

    @staticmethod
    def dump_pickle_files(data):
        if YahooHelper.CACHING_ENABLED:
            for item in data:
                symbol = item["info"]["ticker"]
                pickle_file_path = os.path.join(
                    LUMIBOT_YAHOO_CACHE_PATH, f"{symbol}.pickle"
                )
                if not os.path.exists(pickle_file_path):
                    with open(pickle_file_path, "wb") as f:
                        pickle.dump(item, f)

    @staticmethod
    def format_df(df, auto_adjust):
        if auto_adjust:
            del df["Adj Ratio"]
            del df["Close"]
            del df["Open"]
            del df["High"]
            del df["Low"]
            df.rename(
                columns={
                    "Adj Close": "Close",
                    "Adj Open": "Open",
                    "Adj High": "High",
                    "Adj Low": "Low",
                },
                inplace=True,
            )
        else:
            del df["Adj Ratio"]
            del df["Adj Open"]
            del df["Adj High"]
            del df["Adj Low"]

        return df

    @staticmethod
    def process_df(df):
        df = df.copy()
        df["Adj Ratio"] = df["Adj Close"] / df["Close"]
        df["Adj Open"] = df["Open"] * df["Adj Ratio"]
        df["Adj High"] = df["High"] * df["Adj Ratio"]
        df["Adj Low"] = df["Low"] * df["Adj Ratio"]
        return df

    # ========Data download method=============================

    @staticmethod
    def download_symbol_info(symbol):
        ticker = yf.Ticker(symbol)
        try:
            ticker._get_fundamentals()
        except (ValueError, KeyError):
            return {
                "ticker": ticker.ticker,
                "error": True,
            }

        try:
            options = ticker.options
        except IndexError:
            options = ()

        return {
            "ticker": ticker.ticker,
            "error": False,
            "recommendations": ticker.recommendations,
            "calendar": ticker.calendar,
            "major_holders": ticker.major_holders,
            "institutional_holders": ticker.institutional_holders,
            "info": ticker.info,
            "sustainability": ticker.sustainability,
            "earnings": ticker.earnings,
            "quarterly_earnings": ticker.quarterly_earnings,
            "financials": ticker.financials,
            "quarterly_financials": ticker.quarterly_financials,
            "balance_sheet": ticker.balance_sheet,
            "quarterly_balance_sheet": ticker.quarterly_balance_sheet,
            "cashflow": ticker.cashflow,
            "quarterly_cashflow": ticker.quarterly_cashflow,
            "isin": ticker.isin,
            "options": options,
        }

    @staticmethod
    def download_symbol_data(symbol):
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="max", auto_adjust=False)
        df = YahooHelper.process_df(df)
        return df

    @staticmethod
    def download_symbols_data(symbols):
        if len(symbols) == 1:
            item = YahooHelper.download_symbol_data(symbols[0])
            return {symbols[0]: item}

        result = {}
        tickers = yf.Tickers(" ".join(symbols))
        data = tickers.history(period="max", auto_adjust=False, progress=False)
        data = data.swaplevel(axis=1)
        for symbol in symbols:
            df = data[symbol]
            df = YahooHelper.process_df(df)
            result[symbol] = df
        return result

    @staticmethod
    def download_symbol(symbol):
        info = YahooHelper.download_symbol_info(symbol)
        df = YahooHelper.download_symbol_data(symbol)
        return {
            "data": df,
            "info": info,
            "last_update": get_lumibot_datetime(),
        }

    @staticmethod
    def download_symbols(symbols):
        result = []
        data = YahooHelper.download_symbols_data(symbols)
        for symbol, df in data.items():
            info = YahooHelper.download_symbol_info(symbol)
            result.append(
                {
                    "data": df,
                    "info": info,
                    "last_update": get_lumibot_datetime(),
                }
            )
        return result

    # =======Cache retrieval and dumping=====================

    @staticmethod
    def fetch_symbol(symbol):
        data = YahooHelper.check_pickle_file(symbol)
        if data:
            return data

        data = YahooHelper.download_symbol(symbol)
        YahooHelper.dump_pickle_file(symbol, data)
        return data

    @staticmethod
    def fetch_symbols(symbols):
        result = []
        missing_symbols = []

        for symbol in symbols:
            data = YahooHelper.check_pickle_file(symbol)
            if data:
                result.append(data)
            else:
                missing_symbols.append(symbol)

        if missing_symbols:
            missing_data = YahooHelper.download_symbols(missing_symbols)
            result.extend(missing_data)
            YahooHelper.dump_pickle_files(missing_data)

        return result

    # ======Data getters======================================

    @staticmethod
    def get_symbol_info(symbol):
        data = YahooHelper.fetch_symbol(symbol)
        return data["info"]

    @staticmethod
    def get_symbol_data(symbol, auto_adjust=True):
        data = YahooHelper.fetch_symbol(symbol)
        df = data["data"]
        df = YahooHelper.format_df(df, auto_adjust)
        return df

    @staticmethod
    def get_symbols_data(symbols, auto_adjust=True):
        if len(symbols) == 1:
            return YahooHelper.get_symbol_data(symbols[0], auto_adjust=auto_adjust)

        result = {}
        data = YahooHelper.fetch_symbols(symbols)
        for item in data:
            symbol = item["info"]["ticker"]
            df = item["data"]
            df = YahooHelper.format_df(df, auto_adjust)
            result[symbol] = df

        return result

    # ======Shortcut methods==================================

    @staticmethod
    def get_symbol_dividends(symbol):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.get_symbol_data(symbol)
        dividends = history["Dividends"]
        return dividends[dividends != 0].dropna()

    @staticmethod
    def get_symbols_dividends(symbols):
        result = {}
        data = YahooHelper.get_symbols_data(symbols)
        for symbol, df in data.items():
            dividends = df["Dividends"]
            result[symbol] = dividends[dividends != 0].dropna()

        return result

    @staticmethod
    def get_symbol_splits(symbol):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.get_symbol_data(symbol)
        splits = history["Stock Splits"]
        return splits[splits != 0].dropna()

    @staticmethod
    def get_symbols_splits(symbols):
        result = {}
        data = YahooHelper.get_symbols_data(symbols)
        for symbol, df in data.items():
            splits = df["Stock Splits"]
            result[symbol] = splits[splits != 0].dropna()

        return result

    @staticmethod
    def get_symbol_actions(symbol):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.get_symbol_data(symbol)
        actions = history[["Dividends", "Stock Splits"]]
        return actions[actions != 0].dropna(how="all").fillna(0)

    @staticmethod
    def get_symbols_actions(symbols):
        result = {}
        data = YahooHelper.get_symbols_data(symbols)
        for symbol, df in data.items():
            actions = df[["Dividends", "Stock Splits"]]
            result[symbol] = actions[actions != 0].dropna(how="all").fillna(0)

        return result

    @staticmethod
    def get_risk_free_rate(with_logging=True):
        # 13 Week Treasury Rate (^IRX)
        irx_data = YahooHelper.get_symbol_info("^IRX")
        risk_free_rate = irx_data["info"]["regularMarketPrice"] / 100
        if with_logging:
            logging.info(f"Risk Free Rate {risk_free_rate * 100:0.2f}%")

        return risk_free_rate

    @staticmethod
    def get_trading_days(with_logging=True):
        """Requesting data for the oldest company,
        Consolidated Edison from yahoo finance.
        Storing the trading days."""
        if with_logging:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
            logging.info("Fetching past trading days")

        data = YahooHelper.get_symbol_data("ED")
        days = [d.date() for d in data.index]
        return days
