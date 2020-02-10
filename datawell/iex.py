"""
Contains Iex class which retrieves information from IEX API
"""

from decimal import Decimal
from datetime import datetime, timedelta
import requests
import app


class Iex(object):

    def __init__(self):
        self.dict_symbols = {}
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()
        # now takes too long time - that was the idea :)
        self.get_astats()
        self.populate_dividends()
        self.get_company()
        datapoints = ['logo', 'company']
        self.Datapoints = dict(zip(datapoints, datapoints))

    def __format__(self, format):
        return "\n".join(f"symbol {s} with data {d}"
                         for s, d in self.Symbols.items() or {})

    def get_astats(self, tickers: dict = {}):
        """
        Will return all the advanced stats for tickers
            or for all in self.Symbols
        :return: True and populate self.Symbols with price to book,
            raises AppException if encountered an error
        """
        try:
            self.Logger.debug(f'update stats for {tickers}')
            upd = self.Symbols if not tickers else tickers
            for stock, data in upd.items():
                uri = (f'{app.BASE_API_URL}'
                       f'stock/{stock}/'
                       f'advanced-stats/{app.API_TOKEN}')
                result = self.load_from_iex(uri=uri)
                self.Logger.debug(
                    f'advanced stats for {stock} is {result}')
                data.update(ADVANCED_STATS=result)
            return upd

        except Exception as e:
            message = 'Failed while retrieving advanced stats!'
            ex = app.AppException(e, message)
            raise ex

    def get_books(self, *symbols):
        """
        The method for IEX populates BOOK data either for all stocks (if no ticker given) or a particular stock (if ticker given).
        After method call, particular ticker has a dict added to it with data returned by the call.
        Here is API endpoint data: https://iexcloud.io/docs/api/#book
        _________________________________________
        The method updates existing self.Symbols list with retrieved BOOK data.
        """
        try:
            symbols = set(symbols)
            if len(symbols) == 0:
                self.Logger.info("No symbols have been provided to get BOOK data. All symbols will be used to call BOOK data.")
                if(len(self.Symbols) == 0):
                    self.Logger.warning("Nothing to update. Symbols List is blank.")
                else:
                    [symbol.update({
                        'BOOK':
                        (self.load_from_iex(f'{app.BASE_API_URL}stock/{symbol.get("symbol")}/book/{app.API_TOKEN}'))
                        }) for symbol in self.Symbols]
                self.Logger.info("All symbols have been updated with BOOK data.")
            else:
                # Check if there are not existing symbols among inpput parameters:
                [self.Logger.warning(f'There is no {symbol} symbol among IEX Symbols list.')
                for symbol in symbols
                if not any(
                    Symbol['symbol'] == symbol for Symbol in self.Symbols
                    )]
                # Update all existing tinkers with BOOK data:
                [Symbol.update({
                    'BOOK':
                    (self.load_from_iex(f'{app.BASE_API_URL}stock/{symbol}/book/{app.API_TOKEN}'))
                    }) for Symbol in self.Symbols
                    for symbol in symbols
                    if Symbol.get('symbol') == symbol]
        except Exception as e:
            message = 'Failed while retrieving BOOKS list!'
            ex = app.AppException(e, message)
            raise ex

    def get_stocks(self):
        """
        Will return all the stocks being traded on IEX.
        :return: list of stock tickers and basic facts as list(),
            raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            uri = f'{app.BASE_API_URL}ref-data/Iex/symbols/{app.API_TOKEN}'
            [self.dict_symbols.update({stock.pop("symbol"): {"symbol": stock}})
             for stock in self.load_from_iex(uri)]
            return self.dict_symbols

        except Exception as e:
            message = 'Failed while retrieving stock list!'
            ex = app.AppException(e, message)
            raise ex

    def populate_dividends(self, ticker: str = None, period: str = '1y'):
        """
        Populates symbols with dividents info
        :param ticker: str with ticker that should be populated, if None all the tickers are populated
        :param period: str with period, 1y is default value
        :return: Nothing
        """
        self.Logger.info("Populate symbols with dividents")
        # TODO we might want to do that in parallel, but I am not sure if that is not part of optimization that should
        # be done later
        try:
            for company_info in self.Symbols:
                company_symbol = company_info['symbol']
                if ticker is None or company_symbol == ticker:
                    uri = f'{app.BASE_API_URL}stock/{company_symbol}/dividends/{period}{app.API_TOKEN}'
                    company_info['dividends'] = self.load_from_iex(uri)
        except Exception as e:
            message = f'Failed while retrieving dividends for ticker {ticker}!'
            ex = app.AppException(e, message)
            raise ex

    def get_company(self, Symbols: dict = {}):
        """
        Will return companies according to the stocks or all stocks.
        https://github.com/petelind/ConsumingAPI/issues/2
        """
# if noone symbol is not provided, default set of symbols(all) will be used
        Symbols = self.Symbols if not Symbols else Symbols
        try:
            # company_list = []
            for symbol, symbol_data in Symbols.items():
                self.Logger.debug(f'Update {symbol} symbol with company info.')
                uri = f'{app.BASE_API_URL}stock/{symbol}/company{app.API_TOKEN}'
                company_info = self.load_from_iex(uri)
            # poppulate with company info
                symbol_data.update(company=company_info)
            return Symbols
        except Exception as e:
            message = 'Failed while retrieving companies!'
            ex = app.AppException(e, message)
            raise ex

    @app.retry(app.AppException, logger=app.get_logger(__name__))
    def load_from_iex(self, uri: str):
        """
        Connects to the specified IEX endpoint and gets the data you requested.
        :type uri: str with the endpoint to query
        :return Dict() with the answer from the endpoint, Exception otherwise
        """
        try:
            self.Logger.info(f'Now retrieveing from {uri}')
            response = requests.get(uri)
            response.raise_for_status()
            company_info = response.json(parse_float=Decimal)
            self.Logger.debug(f'Got response: {company_info}')
            return company_info
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                raise app.AppException(e, message="Too Many Requests")
            else:
                self.Logger.error(
                    f'Encountered an error: {response.status_code}'
                    f'( {response.text} ) while retrieving {uri}')
                raise e
