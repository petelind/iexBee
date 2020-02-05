"""
Contains Iex class which retrieves information from IEX API
"""

import json
from decimal import Decimal
import requests
import app


class Iex(object):

    def __init__(self):
        self.stock_list = []
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()
        # now takes too long time - that was the idea :)
        self.get_astats()
        # Commented as it takes quite long time to get all the dividents sequentially
        self.populate_dividends()
        datapoints = ['logo', 'company']
        self.Datapoints = dict(zip(datapoints, datapoints))

    def __format__(self, format): 
        # This funny thing is called list comprehension and is a damn fast iterator...
        # Here is how it works: https://nyu-cds.github.io/python-performance-tips/08-loops/
        return "\n".join(f"{s}"  for s in self.Symbols )
        #                  ^ operand ^ subject  ^iterable 
        # (collection or whatever is able to __iter()__)

    def get_astats(self, tickers: list = []):
        """
        Will return all the advanced stats for tickers
            or for all in self.Symbols
        :return: True and populate self.Symbols with price to book,
            raises AppException if encountered an error
        """
        try:
            self.Logger.debug(f'update stats for {tickers}')
            for stock in self.Symbols:
                if not tickers or stock.get('symbol') not in tickers:
                    continue
                uri = (f'{app.BASE_API_URL}'
                       f'stock/{stock.get("symbol")}/'
                       f'advanced-stats/{app.API_TOKEN}')
                result = self.load_from_iex(uri=uri)
                self.Logger.debug(
                    f'advanced stats for {stock.get("symbol")} is {result}')
                stock.update(priceToBook=result.get('priceToBook'))
            return True

        except Exception as e:
            message = 'Failed while retrieving advanced stats!'
            ex = app.AppException(e, message)
            raise ex

    def get_books(self,*symbols):
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
                #Check if there are not existing symbols among inpput parameters:
                [self.Logger.warning(f'There is no {symbol} symbol among IEX Symbols list.')
                for symbol in symbols
                if not any(
                    Symbol['symbol'] == symbol for Symbol in self.Symbols
                    )]
                #Update all existing tinkers with BOOK data:
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
        :return: list of stock tickers and basic facts as list(), raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            uri = f'{app.BASE_API_URL}ref-data/Iex/symbols/{app.API_TOKEN}'
            self.stock_list = self.load_from_iex(uri)
            return self.stock_list

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
        #TODO we might want to do that in parallel, but I am not sure if that is not part of optimization that should
        #be done later
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

    def load_from_iex(self, uri: str):
        """
        Connects to the specified IEX endpoint and gets the data you requested.
        :type uri: str with the endpoint to query
        :return Dict() with the answer from the endpoint, Exception otherwise
        """
        self.Logger.info(f'Now retrieveing from {uri}')
        response = requests.get(uri)
        if response.status_code == 200:
            company_info = json.loads(response.content.decode("utf-8"), parse_float=Decimal)
            self.Logger.debug(f'Got response: {company_info}')
            return company_info
        else:
            error = response.status_code
            self.Logger.error(
                f'Encountered an error: {error} ( {response.text} ) while retrieving {uri}')
