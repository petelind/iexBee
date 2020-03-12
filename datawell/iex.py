"""
Contains Iex class which retrieves information from IEX API
"""

from decimal import Decimal
import requests
import app



class Iex(object):

    def __init__(self, symbols: dict = {} ):
        self.dict_symbols = {}
        self.Logger = app.get_logger(__name__)
        self.Symbols = symbols if symbols else self.get_stocks()
        self.datapoints = [
            'advanced-stats', 'cash-flow', 'book',
            'dividends', 'company', 'financials'
        ]
        self.get_symbols_batch(datapoints=self.datapoints,symbols=self.Symbols)

    def get_symbols(self):
        return self.Symbols.values()

    def __format__(self, format):
        return "\n".join(f"symbol {s} with data {d}"
                         for s, d in self.Symbols.items() or {})


    def get_stocks(self):
        """
        Will return all the stocks being traded on IEX.
        :return: list of stock tickers and basic facts as list(),
            raises AppException if encountered an error
        """
        try:
            # basically we create a market snapshot
            uri = f'{app.BASE_API_URL}ref-data/Iex/symbols/'
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

    @app.retry(app.AppException, logger=app.get_logger(__name__))
    @app.dict_cleanup
    def load_from_iex(self, uri: str):
        """
        Connects to the specified IEX endpoint and gets the data you requested.
        :type uri: str with the endpoint to query
        :return Dict() with the answer from the endpoint, Exception otherwise
        """
        try:
            self.Logger.info(f'Now retrieveing from {uri}')
            params = {'token': app.API_TOKEN}
            response = requests.get(url=uri, params=params)
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

    @app.batchify(param_to_slice='datapoints', size=10)
    @app.batchify(param_to_slice='symbols', size=100)
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
            self.Logger.debug(
                f'Following tickers: {tickers}'
                f'will be populated with data from endpoints: {datapoints}.'
            )
            uri = (
                f'{app.BASE_API_URL}stock/market/batch?symbols={tickers}&'
                f'types={types}&range=1m&last=5'
            ).encode('utf8')


            result = self.load_from_iex(uri)
            if result:
                [symbols[key].update(val) for key, val in result.items()]

        except Exception as e:
            message = 'Failed while retrieving batch request data!'
            ex = app.AppException(e, message)
            raise ex
