"""
Contains Iex class which retrieves information from IEX API
"""

from decimal import Decimal
import requests
import app
import logging
from itertools import islice
from urllib import parse


def split_request(func):
    def batch_wrapper(self, symbols, datapoints):
        ticker_chunk = 100
        datapoint_chunk = 10
        split_symbols = split_dict(symbols, ticker_chunk)
        split_datapoints = split_list(datapoints, datapoint_chunk)
        for symbols in split_symbols:
            for datapoints in split_datapoints:
                func(self, symbols, datapoints)

    def split_dict(data, size):
        it = iter(data)
        for _ in range(0, len(data), size):
            yield {k: data[k] for k in islice(it, size)}

    def split_list(data, size):
        return [data[x:x + size] for x in range(0, len(data), size)]

    return batch_wrapper


class Iex(object):

    def __init__(self, symbols: dict = {}, log_level=logging.INFO):
        self.log_level = log_level
        self.dict_symbols = {}
        self.Logger = app.get_logger(__name__, level=self.log_level)
        self.Symbols = symbols if symbols else self.get_stocks()
        self.datapoints = [
            'advanced-stats', 'cash-flow', 'book',
            'dividends', 'company', 'financials'
        ]
        self.get_symbols_batch(self.Symbols, self.datapoints)

    def get_symbols(self):
        return list(self.Symbols.values())

    def __format__(self, format):
        return "\n".join(f"symbol {s} with data {d}"
                         for s, d in self.Symbols.items() or {})

    def __make_uri(self, uri_special_bones):
        """
        The function is a local support function and it creates a list of 6 url encoded items.
        The list changes into url string with function urlunparse
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlunparse
        return a list with 6 parts of url
        """
        # a default uri parts aka 'bones'
        uri_bones_default = {
            "scheme": f"{app.BASE_API_URL.split('://')[0]}",
            "netloc": f"{app.BASE_API_URL.split('://')[1]}",
            "path": "",
            "params": {},
            "query": {},
            "fragment": ""
        }
        # add special part of a request
        uri_bones = {**uri_bones_default, **uri_special_bones}
        # add token to the result query
        uri_bones["query"]["token"] = f"{app.API_TOKEN}"
        uri_skeleton = [
            uri_bones['scheme'],
            uri_bones['netloc'],
            uri_bones['path'],
            parse.urlencode(uri_bones['params']),
            parse.urlencode(uri_bones['query']),
            uri_bones['fragment']
        ]
        return uri_skeleton

    @app.func_time(logger=app.get_logger(__name__))
    def get_stocks(self):
        """
        Will return all the stocks being traded on IEX.
        :return: list of stock tickers and basic facts as list(),
            raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            # a dict of different than default parameters for a result uri in a readable format
            uri_special_bones = {
                "path": "/ref-data/Iex/symbols/"
            }
            [
                self.dict_symbols.update(
                    {stock.get("symbol"): app.remove_empty_strings(stock)}
                ) for stock in self.load_from_iex(self.__make_uri(uri_special_bones))
            ]
            return self.dict_symbols

        except Exception as e:
            message = 'Failed while retrieving stock list!'
            ex = app.AppException(e, message)
            raise ex

    @app.retry(app.AppException, logger=app.get_logger(__name__))
    @app.deco_dict_cleanup
    @app.func_time(logger=app.get_logger(__name__))
    def load_from_iex(self, uri_skeleton: list):
        """
        Connects to the specified IEX endpoint and gets the data you requested.
        :type uri: str with the endpoint to query
        :return Dict() with the answer from the endpoint, Exception otherwise
        """
        try:
            uri = parse.urlunparse(uri_skeleton)
            self.Logger.info(f'Now retrieveing from {uri}')
            response = requests.get(url=uri)
            response.raise_for_status()
            company_info = response.json(parse_float=Decimal)
            self.Logger.debug(f'Got response: {company_info}')
            return company_info
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                raise app.AppException(e, message="Too Many Requests")
            if response.status_code == 404 and response.text == 'Unknown symbol':
                self.Logger.warning(f'Unknown symbol error while retrieving {uri}')
            else:
                self.Logger.error(
                    f'Encountered an error: {response.status_code}'
                    f'( {response.text} ) while retrieving {uri}')
                raise e

    @split_request
    @app.func_time(logger=app.get_logger(__name__))
    def get_symbols_batch(self, symbols: dict, datapoints: list):
        """
        Updates Symbols dict with specified datapoints.
        Url example to get data in batch:
        https://sandbox.iexapis.com/stable/stock/market/batch?
        symbols=aapl,fb&types=quote,news,chart&range=1m&last=5&
        token=Tsk_d74d55cc782642a0ad8f1779ad6f0098

        `symbols` - comma delimited list of symbols limited to 100.
        This parameter is used only if market option is used.

        `types` (locally: datapoints) - comma delimited list of endpoints to call.
        The names should match the individual endpoint names.
        Limited to 10 endpoints.

        `range`
        Used to specify a chart range if chart is used in types parameter.

        `*`
        Parameters that are sent to individual endpoints can be specified in batch calls
        and will be applied to each supporting endpoint. For example,
        `last` can be used for the news endpoint to specify the number of articles
        """

        def array_to_string(data):
            return ','.join([key for key in data]).lower()

        try:
            symbols = self.Symbols if not symbols else symbols
            tickers = array_to_string(symbols)
            types = array_to_string(datapoints)

            self.Logger.info("Populate symbols with whole data set.")
            self.Logger.debug(
                f'Following tickers: {tickers}'
                f'will be populated with data from endpoints: {types}.'
            )
            uri_special_bones = {
                "path": "/stock/market/batch",
                "query": {
                    "symbols": tickers,
                    "types": types,
                    "range": "1m",
                    "last": 5
                }
            }
            result = self.load_from_iex(self.__make_uri(uri_special_bones))
            if result:
                [symbols[key].update(val) for key, val in result.items()]

        except Exception as e:
            message = 'Failed while retrieving batch request data!'
            ex = app.AppException(e, message)
            raise ex
