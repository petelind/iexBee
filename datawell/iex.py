"""
Contains Iex class which retrieves information from IEX API
"""

from decimal import Decimal
import requests
import app
from itertools import islice


def split_request(func):
    def batch_wrapper(self, symbols, datapoints):
        ticker_chunk = 2
        datapoint_chunk = 2
        split_symbols = split_dict(symbols, ticker_chunk)
        split_datapoints = split_list(datapoints, datapoint_chunk)
        #[func(self, symbols, datapoints) for datapoints in split_datapoints for symbols in split_symbols]
        for symbols in split_symbols:
            for datapoints in split_datapoints:
                #print(f'Symbols {symbols} for {datapoints}')
                func(self, symbols, datapoints)

    def split_dict(data, size):
        it = iter(data)
        for i in range(0, len(data), size):
            yield {k: data[k] for k in islice(it, size)}

    def split_list(data, size):
        return [data[x:x+size] for x in range(0, len(data), size)]

    return batch_wrapper


class Iex(object):

    def __init__(self, symbols: dict = {} ):
        self.dict_symbols = {}
        self.Logger = app.get_logger(__name__)
        self.Symbols = symbols if symbols else self.get_stocks()
        # now takes too long time - that was the idea :)
        self.get_astats()
        self.populate_dividends()
        self.get_company()
        self.get_financials()
        self.get_cash_flow()
        self.get_books()
        # self.get_symbols_batch(self.Symbols, ['advanced-stats','cash-flow','book','dividends','company','financials'])
        datapoints = ['logo', 'company']
        self.Datapoints = dict(zip(datapoints, datapoints))

    def get_symbols(self):
        return self.Symbols.values()

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
                       f'advanced-stats/?{app.API_TOKEN}')
                result = self.load_from_iex(uri=uri)
                self.Logger.debug(
                    f'advanced stats for {stock} is {result}')
                data['advanced-stats'] = result
                app.remove_empty_strings(data)
            return upd

        except Exception as e:
            message = 'Failed while retrieving advanced stats!'
            ex = app.AppException(e, message)
            raise ex

    def get_cash_flow(self, symbols: dict = {}):
        """
        The method for IEX which will populate CASH FLOW data either for all stocks (if no ticker given)
        or a particular stock (if ticker given).
        After method call, particular ticker should have a dict added to it with data returned by the call.
        Here is API endpoint data:
        https://iexcloud.io/docs/api/#cash-flow
        """
        try:
            symbols = self.Symbols if not symbols else symbols
            self.Logger.info("Populate symbols with cash-flow data.")
            [
                symbol_data.update(
                  book=app.remove_empty_strings(self.load_from_iex(
                    f'{app.BASE_API_URL}stock/{symbol}/cash-flow/?{app.API_TOKEN}'
                  ))
                ) for symbol, symbol_data in symbols.items()
            ]
        except Exception as e:
            message = 'Failed while retrieving cash-flow list!'
            ex = app.AppException(e, message)
            raise ex

    def get_books(self, symbols: dict = {}):
        """
        The method for IEX populates BOOK data either for all stocks (if no
        ticker given) or a particular stock (if ticker given). The method
        updates existing self.Symbols list with retrieved BOOK data. Here
        is API endpoint data: https://iexcloud.io/docs/api/#book
        """
        try:
            symbols = self.Symbols if not symbols else symbols
            self.Logger.info("Populate symbols with book data.")
            [
                symbol_data.update(
                  book=app.remove_empty_strings(self.load_from_iex(
                    f'{app.BASE_API_URL}stock/{symbol}/book/?{app.API_TOKEN}'
                  ))
                ) for symbol, symbol_data in symbols.items()
            ]
        except Exception as e:
            message = 'Failed while retrieving books list!'
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
            uri = f'{app.BASE_API_URL}ref-data/Iex/symbols/?{app.API_TOKEN}'
            [
                self.dict_symbols.update(
                    {stock.get("symbol"): app.remove_empty_strings(stock)}
                ) for stock in self.load_from_iex(uri)
            ]
            return self.dict_symbols

        except Exception as e:
            message = 'Failed while retrieving stock list!'
            ex = app.AppException(e, message)
            raise ex

    def populate_dividends(self, tickers: dict = {}, period: str = '1y'):
        """
        Populates symbols with dividents info
        :param tickers: list with tickers that should be populated, if None all the tickers are populated
        :param period: str with period, 1y is default value
        :return: Nothing
        """
        self.Logger.info("Populate symbols with dividents")
        try:
            self.Logger.debug(f'update stats for {tickers}')
            upd = self.Symbols if not tickers else tickers
            for stock, data in upd.items():
                uri = f'{app.BASE_API_URL}stock/{stock}/dividends/{period}?{app.API_TOKEN}'
                data['dividends'] = self.load_from_iex(uri)
                app.remove_empty_strings(data)
        except Exception as e:
            message = f'Failed while retrieving dividends for tickers {tickers}!'
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
                uri = f'{app.BASE_API_URL}stock/{symbol}/company?{app.API_TOKEN}'
                company_info = self.load_from_iex(uri)
            # poppulate with company info
                symbol_data.update(company=company_info)
                app.remove_empty_strings(symbol_data)
            return Symbols
        except Exception as e:
            message = 'Failed while retrieving companies!'
            ex = app.AppException(e, message)
            raise ex

    def get_financials(self, symbols: dict = {}):
        """
        Will return financials data (either all, or for given ticker).
        :type symbols: company symbols data structure to get financials for
        :return: symbols data structure updated with financials
        """

        symbols = self.Symbols if not symbols else symbols
        self.Logger.info("Populate symbols with financials")
        for symbol, data in symbols.items():
            try:
                self.Logger.debug(f'Updating {symbol} symbol with financials.')
                uri = f'{app.BASE_API_URL}stock/{symbol}/financials/{app.API_TOKEN}'
                data["financials"] = self.load_from_iex(uri)["financials"]
                app.remove_empty_strings(data)

            except KeyError:
                # Some symbols don't have financial info associated, so skipping
                continue

            except Exception as e:
                message = 'Failed while retrieving financials!'
                ex = app.AppException(e, message)
                raise ex

        return symbols

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

    @split_request
    def get_symbols_batch(self, symbols: dict, datapoints: list):
        """
        Updates Symbols dict with specified datapoints.
        Url example to get data in batch:
        https://sandbox.iexapis.com/stable/stock/market/batch?
        symbols=aapl,fb&types=quote,news,chart&range=1m&last=5&
        token=Tsk_d74d55cc782642a0ad8f1779ad6f0098

        symbols - comma delimited list of symbols limited to 100.
        This parameter is used only if market option is used.

        datapoints - comma delimited list of endpoints to call.
        The names should match the individual endpoint names.
        Limited to 10 endpoints.
        """

        def array_to_string(data):
            return ','.join([key for key in data]).lower()

        try:

            symbols = self.Symbols if not symbols else symbols
            tickers = array_to_string(symbols)
            types = array_to_string(datapoints)

            self.Logger.info("Populate symbols with whole data set.")
            self.Logger.debug(f'Following tickers: {tickers} will be populated with data from endpoints: {datapoints}.')
            uri = f'{app.BASE_API_URL}stock/market/batch?symbols={tickers}&types={types}&range=1m&last=5&{app.API_TOKEN}'

            result = self.load_from_iex(uri)
            [symbols[key].update(val) for key, val in result.items()]

        except Exception as e:
            message = 'Failed while retrieving batch request data!'
            ex = app.AppException(e, message)
            raise ex