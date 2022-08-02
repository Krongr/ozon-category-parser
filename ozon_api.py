import requests
import json


class APIError(Exception):
    pass

class ConnectionError(APIError):
    def __init__(self, url):
        super().__init__(f'Failed to connect {url}')

class BadResponse(APIError):
    def __init__(self, response):
        try:
            super().__init__(f'{response.json()}')
        except requests.exceptions.JSONDecodeError:
            super().__init__(f'{response.content}')


class OzonApi():
    def __init__(self, client_id, api_key):
        self.api_url = 'https://api-seller.ozon.ru'
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Client-Id': f'{client_id}',
            'Api-key': f'{api_key}',
        }

    def request_category_info(self, category_id:int=None, language='RU'):
        """Returns response with the category name and subcategories.
        If used without 'category_id' returns the full category tree.
        """
        _url = f'{self.api_url}/v2/category/tree'
        _data = {
            'category_id': category_id,
            'language': language,
        }
        try:
            response = requests.post(
                url=_url,
                headers=self.headers,
                data=json.dumps(_data),
            )
        except (
            requests.exceptions.InvalidSchema,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:
            raise ConnectionError(_url) from error

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise BadResponse(response) from error

        return response
    
    def request_category_attributes(self, category_ids:list,
                                    attribute_type='ALL', language='RU'):
        """Returns response with available attributes for
        the specified product categories.
        'category_ids' list should not contain more than 20 entries.
        """
        _url = f'{self.api_url}/v3/category/attribute'
        _data = {
            'attribute_type': attribute_type,
            'category_id': category_ids,
            'language': language,
        }
        try:
            response = requests.post(
                url=_url,
                headers=self.headers,
                data=json.dumps(_data),
            )
        except (
            requests.exceptions.InvalidSchema,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:
            raise ConnectionError(_url) from error

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise BadResponse(response) from error

        return response

    def request_dictionary_values(self, attribute_id:int, category_id:int,
                           last_value_id:int=None, limit=5000, language='RU'):
        """Returns response with a list of dictionary values for
        the specified attribute.
        'last_value_id' can be used to iterate over large lists.
        """
        _url = f'{self.api_url}/v2/category/attribute/values'
        _data = {
            'attribute_id': attribute_id,
            'category_id': category_id,
            'last_value_id': last_value_id,
            'language': language,
            'limit': limit,
        }
        try:
            response = requests.post(
                url=_url,
                headers=self.headers,
                data=json.dumps(_data),
            )
        except (
            requests.exceptions.InvalidSchema,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:
            raise ConnectionError(_url) from error

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise BadResponse(response) from error

        return response
