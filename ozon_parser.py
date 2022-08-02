from sqlalchemy import exc

import app_logger
from ozon_api import OzonApi, ConnectionError, BadResponse


logger = app_logger.create_logger(__name__)

class OzonCategoryParcer(OzonApi):
    def __init__(self, client_id, api_key, db_client,
        category_ids:list=None, dictionary_attributes:list=None):
        super().__init__(client_id, api_key)
        self.client_id = client_id
        self.db_client = db_client
        self.category_ids = category_ids
        self.dictionary_attributes = dictionary_attributes

    def create_category_query(self, category_id:str):
        """Returns a query for adding category info to the DB"""
        try:
            response = self.request_category_info(category_id)
            category_info = response.json()['result'][0]
            query = f"""
                INSERT
                INTO category(
                    "name",
                    cat_id,
                    mp_id
                )
                VALUES(
                    '{category_info['title']}',
                    '{category_info['category_id']}',
                    1
                );
            """
        except BadResponse:
            logger.exception('API method error')
        except ConnectionError:
            logger.exception('Connection error')
        except (
            TypeError,
            KeyError,
            AttributeError,
        ):
            logger.exception('JSON parsing error')
        except Exception:
            logger.exception('Unexpected error')
        else:
            return query

    def create_attribute_query(self, chid, name, is_required, is_collection,
                        type, description, dictionary_id, group_name, cat_id):
        """Returns a query for adding attribute info to the DB"""
        query = f"""
            INSERT
            INTO cat_list(
                chid,
                "name",
                is_required,
                is_collection,
                type,
                description,
                dictionary_id,
                group_name,
                cat_id,
                db_i
            )
            VALUES(
                '{chid}',
                '{name}',
                '{is_required}',
                '{is_collection}',
                '{type}',
                '{description}',
                '{dictionary_id}',
                '{group_name}',
                '{cat_id}',
                '{cat_id}{chid}'
            );                    
        """
        return query.replace(r'%', r'%%')

    def create_category_attributes_queries(self,category_ids:list):
        """Returns a queries for adding categories attributes info to the DB.
        Also returns a set of tuples with categories and
        dictionary attribute ids needed to get dictionary values.
        """
        NAMED_ATTRIBUTES = [
            'complex_attributes', 'color_image', 'barcode',
            'category_id', 'name', 'offer_id', 'height', 'depth',
            'width', 'dimension_unit', 'weight', 'weight_unit',
            'images', 'image_group_id', 'images360', 'pdf_list',
            'description',
        ]
        dictionary_attributes = set()
        queries = []
        try:
            for i in range(0, len(category_ids), 20):
                response = self.request_category_attributes(
                    category_ids[i:i+20]
                )
                category_attributes = response.json()['result']
                for _category in category_attributes:
                    for _attribute in _category['attributes']:
                        queries.append(self.create_attribute_query(
                            _attribute['id'],
                            _attribute['name'],
                            _attribute['is_required'],
                            _attribute['is_collection'],
                            _attribute['type'],
                            _attribute['description'],
                            _attribute['dictionary_id'],
                            _attribute['group_name'],
                            _category['category_id'],
                        ))
                        if _attribute['dictionary_id'] != 0:
                            dictionary_attributes.add(
                                (_attribute['id'], _category['category_id']),
                            )
                    for named_attribute in NAMED_ATTRIBUTES:
                        queries.append(self.create_attribute_query(
                            named_attribute,
                            named_attribute,
                            'True',
                            'False',
                            '',
                            named_attribute,
                            'NULL',
                            'NULL',
                            _category['category_id'],
                        ))
        except BadResponse:
            logger.exception('API method error')
        except ConnectionError:
            logger.exception('Connection error')
        except (
            TypeError,
            KeyError,
            AttributeError,
        ):
            logger.exception('JSON parsing error')
        except Exception:
            logger.exception('Unexpected error')
        finally:
            return queries, dictionary_attributes

    def create_dictionary_value_query(self, value, picture, info,
                                                        attr_param_id, chid):
        """Returns a query for adding dictionary attribute info to the DB"""
        value = str(value).replace("'", "''")
        info = str(info).replace("'", "''")
        query = f"""
            INSERT
            INTO attr_param_list(
                value,
                picture,
                info,
                attr_param_id,
                chid,
                db_i                           
            )
            VALUES(
                '{value}',
                '{picture}',
                '{info}',
                '{attr_param_id}',
                '{chid}',
                '{chid}{attr_param_id}'
            );                    
        """
        return query.replace(r'%', r'%%')

    def commit_dictionary_values(self, attribute_id, category_id):
        """Creates records of the attribute's dictionary values in the DB."""
        last_value_id = None
        try:
            while True:
                queries = []
                response = self.request_dictionary_values(
                    attribute_id,
                    category_id,
                    last_value_id,
                )
                dictionary_values = response.json()['result']
                if dictionary_values:
                    for _value in dictionary_values:
                        queries.append(self.create_dictionary_value_query(
                            _value['value'],
                            _value['picture'],
                            _value['info'],
                            _value['id'],
                            attribute_id,
                    ))

                self.db_client.execute_queries(queries)
                self.db_client.remove_duplicates('attr_param_list', 'db_i')
                if response.json()['has_next']:
                    last_value_id = _value['id']
                else:
                    break
        except exc.SQLAlchemyError:
            logger.exception(f'DB communication error')
        except BadResponse:
            logger.exception('API method error')
        except ConnectionError:
            logger.exception('Connection error')
        except (
            TypeError,
            KeyError,
            AttributeError,
        ):
            logger.exception('JSON parsing error')
        except Exception:
            logger.exception('Unexpected error')
