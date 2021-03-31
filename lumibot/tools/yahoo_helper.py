import logging

import yfinance as yf


class YahooHelper:
    @staticmethod
    def get_risk_free_rate(with_logging=True):
        # 13 Week Treasury Rate (^IRX)
        risk_free_rate_ticker = yf.Ticker("^IRX")
        risk_free_rate = risk_free_rate_ticker.info["regularMarketPrice"] / 100
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

        data = YahooHelper.fetch_symbol_data("ED")
        days = [d.date() for d in data.index]
        return days

    @staticmethod
    def fetch_symbol_info(symbol):
        ticker = yf.Ticker(symbol)
        return {
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
            "options": ticker.options,
        }

    @staticmethod
    def fetch_symbol_data(symbol, auto_adjust=True):
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="max", auto_adjust=auto_adjust)
        return df

    @staticmethod
    def fetch_symbols_data(symbols, auto_adjust=True):
        if len(symbols) == 1:
            return YahooHelper.fetch_symbol_data(symbols[0], auto_adjust=auto_adjust)

        result = {}
        tickers = yf.Tickers(" ".join(symbols))
        data = tickers.history(period="max", auto_adjust=auto_adjust, progress=False)
        data = data.swaplevel(axis=1)
        for symbol in symbols:
            result[symbol] = data[symbol]
        return result

    @staticmethod
    def fetch_data(input, auto_adjust=True):
        if isinstance(input, list):
            return YahooHelper.fetch_symbols_data(input, auto_adjust=auto_adjust)
        return YahooHelper.fetch_symbol_data(input, auto_adjust=auto_adjust)

    @staticmethod
    def get_symbol_dividends(symbol):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.fetch_symbol_data(symbol)
        dividends = history["Dividends"]
        return dividends[dividends != 0].dropna()

    @staticmethod
    def get_symbols_dividends(symbols):
        result = {}
        data = YahooHelper.fetch_symbols_data(symbols)
        for symbol, df in data.items():
            dividends = df["Dividends"]
            result[symbol] = dividends[dividends != 0].dropna()

        return result

    @staticmethod
    def get_symbol_splits(symbol):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.fetch_symbol_data(symbol)
        splits = history["Stock Splits"]
        return splits[splits != 0].dropna()

    @staticmethod
    def get_symbols_splits(symbols):
        result = {}
        data = YahooHelper.fetch_symbols_data(symbols)
        for symbol, df in data.items():
            splits = df["Stock Splits"]
            result[symbol] = splits[splits != 0].dropna()

        return result

    @staticmethod
    def get_symbol_actions(symbol):
        """https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py"""
        history = YahooHelper.fetch_symbol_data(symbol)
        actions = history[["Dividends", "Stock Splits"]]
        return actions[actions != 0].dropna(how="all").fillna(0)

    @staticmethod
    def get_symbols_actions(symbols):
        result = {}
        data = YahooHelper.fetch_symbols_data(symbols)
        for symbol, df in data.items():
            actions = df[["Dividends", "Stock Splits"]]
            result[symbol] = actions[actions != 0].dropna(how="all").fillna(0)

        return result
