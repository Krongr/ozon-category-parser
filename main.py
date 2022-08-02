import math
from sqlalchemy import exc
from concurrent.futures import ThreadPoolExecutor

from db_client import DbClient
from ozon_parser import OzonCategoryParcer
import app_logger


logger = app_logger.create_logger(__name__)

# DB settings:
TYPE= ''
DB_NAME= ''
HOST= ''
PORT= ''
USER= ''
PASSWORD= ''

def run_category_parser(parser:OzonCategoryParcer):
    _id = parser.client_id
    valid_categories = list()
    try:
        category_queries = []
        for category_id in parser.category_ids:
            if category_id != '0':
                query = parser.create_category_query(category_id)
                if query:
                    category_queries.append(query)
                    valid_categories.append(category_id)
        parser.db_client.execute_queries(category_queries)
        parser.db_client.remove_duplicates('category', 'cat_id, mp_id')

        attribute_queries, dictionary_attributes = (
            parser.create_category_attributes_queries(valid_categories)
        )
        parser.db_client.execute_queries(attribute_queries)
        parser.db_client.remove_duplicates('cat_list', 'db_i')

        logger.info(f'Categories info commited ({_id})')

    except exc.SQLAlchemyError:
        logger.exception(f'DB communication error')
    except Exception:
        logger.exception(f'Unexpected error')
    finally:
        return dictionary_attributes

def run_dictionary_parser(parser:OzonCategoryParcer):
    _id = parser.client_id

    try:
        for attribute, category in parser.dictionary_attributes:
            parser.commit_dictionary_values(attribute, category)
        logger.info(f'Dictionary values commited ({_id})')
    except exc.SQLAlchemyError:
        logger.exception(f'DB communication error')
    except Exception:
        logger.exception(f'Unexpected error')


if __name__ == "__main__":
    logger.info(f'Script started')
    db_client = DbClient(TYPE, DB_NAME, HOST, PORT, USER, PASSWORD)

    try:
        credentials = db_client.get_credentials()
        category_ids = db_client.get_category_ids()
    except exc.SQLAlchemyError:
        logger.exception('Getting data from DB failed')
    logger.info(f'Category IDs collected')
    part_size = math.ceil(len(category_ids) / len(credentials))

    category_parsers = list()
    for i, _entry in enumerate(credentials):
        category_parsers.append(OzonCategoryParcer(
            _entry['client_id'],
            _entry['api_key'],
            db_client,
            category_ids=category_ids[(part_size*(i)):(part_size*(i+1))],
        ))

    with ThreadPoolExecutor(len(credentials)) as executor:
        result = executor.map(run_category_parser, category_parsers)

    dictionary_attributes = set()
    for _entry in result:
        dictionary_attributes = dictionary_attributes.union(_entry)
    dictionary_attributes = list(dictionary_attributes)
    part_size = math.ceil(len(dictionary_attributes) / len(credentials))

    dictionary_parsers = list()
    for i, _entry in enumerate(credentials):
        dictionary_parsers.append(OzonCategoryParcer(
            _entry['client_id'],
            _entry['api_key'],
            db_client,
            dictionary_attributes=(
                dictionary_attributes[(part_size*(i)):(part_size*(i+1))]
            ),
        ))

    with ThreadPoolExecutor(len(credentials)) as executor:
        executor.map(run_dictionary_parser, dictionary_parsers)
    
    logger.info(f'Script completed')
