from sqlalchemy import create_engine, exc


class DbClient():
    def __init__(self, db_type, db_name, host, port, user, password):
        self.db = (
            f'{db_type}://{user}:{password}@{host}:{port}/{db_name}'
        )
        self.engine = create_engine(self.db)

    def get_credentials(self)->list:
        credentials = []
        with self.engine.connect() as connection:
            try:
                response = connection.execute(f"""
                    SELECT DISTINCT client_id_api, api_key
                    FROM account_list  
                    WHERE mp_id = 1 AND
                        client_id_api IS NOT NULL AND
                        client_id_api != '';
                """).fetchall()
                for _item in response:
                    credentials.append({
                        'client_id': _item[0],
                        'api_key': _item[1],
                    })
            except exc.SQLAlchemyError:
                raise
            finally:
                connection.close()
                return credentials

    def get_category_ids(self)->list:
        category_ids = []
        with self.engine.connect() as connection:
            try:
                response = connection.execute(f"""
                    SELECT DISTINCT value
                    FROM product_attr
                    WHERE attribute_id = 'category_id';
                """).fetchall()
                for _item in response:
                    category_ids.append(_item[0])
            except exc.SQLAlchemyError:
                raise
            finally:
                connection.close()
                return category_ids

    def execute_queries(self, queries:list):
        with self.engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    for query in queries:
                        connection.execute(query)
                except exc.SQLAlchemyError:
                    transaction.rollback()
                    raise
                else:
                    transaction.commit()
                finally:
                    connection.close()


    def remove_duplicates(self, table, partition):
        with self.engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    connection.execute(f"""
                        DELETE
                        FROM {table}
                        WHERE ctid IN (
                            SELECT ctid
                            FROM (
                                SELECT *, ctid, row_number()
                                OVER (
                                    PARTITION BY {partition}
                                    ORDER BY id DESC
                                )
                                FROM {table}
                            ) as subquery
                            WHERE row_number != 1
                        );
                    """)
                except exc.SQLAlchemyError:
                    transaction.rollback()
                    raise
                else:
                    transaction.commit()
                finally:
                    connection.close()
