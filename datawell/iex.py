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
        datapoints = ['logo', 'company']
        self.Datapoints = dict(zip(datapoints, datapoints))

    def __format__(self, format): 
        # This funny thing is called list comprehension and is a damn fast iterator...
        # Here is how it works: https://nyu-cds.github.io/python-performance-tips/08-loops/
        return "\n".join(f"{s}"  for s in self.Symbols )
        #                  ^ operand ^ subject  ^iterable 
        # (collection or whatever is able to __iter()__)

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
