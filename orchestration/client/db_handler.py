import psycopg2
import logging
from typing import List, Dict, Any

class DBHandler:
    def __init__(self, config: Dict[str, str], dbname:str = 'clima', schema:str = 'clima_schema'):
        self.config = config
        self.logger = init_logger(self)
        self.dbname = dbname
        self.schema = schema
        self.exists = self.check_db()
        
        if self.exists:
            self.logger.info('Database already exists')
        else:
            self.logger.info('Database does not exist')
            self.create_db()
            self.create_schema()
            self.create_tables()
    
    def init_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG) 
        file_handler = logging.FileHandler('app.log')
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(self.formatter) 
        logger.addHandler(self.file_handler)
        
        return logger

    def get_cursor(self):
        connection = None
        cursor = None

        try:
            connection = psycopg2.connect(**self.config)
            cursor = connection.cursor()

            yield cursor

            cursor.commit()
            conneciton.commit()
        except Exception as e:
            self.logger.error(f'Error while using the cursor: {e}')
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def prepare_insertion_query(self, table, columns):
        self.logger.info(f'Starting the creation of insertion query @ {table} with {columns}')
        query = f'INSERT INTO {table} '
        query = query.join(columns)
        query += 'VALUES ('
        for item in columns:
            query += '%s, '
        query += ')'
        self.logger.info(f'Done the creation of the following query:{query}')
        return query
    
    def insert_one(self, table, columns, value):
        try:
            self.logger.info(f'Starting the insertion @ {table} with {columns} of 1 row')
            query = self.prepare_query(table, columns)
            with self.get_cursor() as cursor:
                cursor.execute(query, value)
            self.logger.info(f'Done the insertion of {value}')
            return True
        except Exception as e:
            self.logger.error(f'Error inserting @ {table} with {columns}: {e}')
            return False

    def insert_multiple(self, table, columns, values):
        try:
            self.logger.info(f'Starting the insertion @ {table} with {columns} of {len(values)} rows')
            query = self.prepare_query(table, columns)
            with self.get_cursor() as cursor:
                cursor.executemany(query, values)
            self.logger.info(f'Done the insertion @ {len(values)} rows')
            return True
        except Exception as e:
            self.logger.error(f'Error inserting @ {table} with {columns} of {len(values)} rows: {e}')
            return False

    def get_data(self, table, columns, table):
        try:
            self.logger.info(f'Getting data from {columns} from {table}')
            query = f"SELECT {' '.join(columns)} FROM {table}"
            with self.get_cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
            self.logger.info(f'Got {len(data)} rows from {table}')
            return data
        except Exception as e:
            self.logger.error(f'Error getting data from {table}: {e}')
            return None
    
    def check_db(self,):
        with self.get_cursor() as cursor:
            cursor.execute(f'SELECT 1 FROM pg_database WHERE datname = {self.dbname};')
        self.exists = cursor.fetchone() is not None
        self.logger.info(f'Database {self.dbname} exists: {self.exists}')
        return self.exists

    def create_db(self):
        try:
            self.logging.info(f'Creating database {self.dbname}')
            with self.get_cursor() as cursor:
                cursor.execute(f'CREATE DATABASE {self.dbname}')
            self.logging.info(f'Database {self.dbname} created')
            return True
        except Exception as e:
            logger.error(f'Error creating the db: {e}')
            return False

    def create_schema(self):
        try:
            self.logging.info(f'Creating schema {self.schema}')
            with self.get_cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA {self.schema}')
            self.logging.info(f'Schema {self.schema} created')
            return True
        except Exception as e:
            logger.error(f'Error creating the schema: {e}')
            return False

    def create_tables(self):
        try:
            self.logging.info(f'creating tables')
            with self.get_cursor() as cursor:
                cursor.execute(f'''
                CREATE TABLE {self.schema}.cidades(
                    cidade VARCHAR(255),
                    uf VARCHAR(2),
                    id INT PRIMARY KEY
                )
                ''')
                
                cursor.execute(f'''
                CREATE TABLE {self.schema}.metar(
                    estacao VARCHAR(4) PRIMARY KEY,
                    data DATE DEFAULT CURRENT_DATE,
                    pressao FLOAT,
                    tempo VARCHAR(4),
                    temp_desc VARCHAR(255),
                    umidade FLOAT,
                    vento_dir VARCHAR(255),
                    vento_int FLOAT,
                    visibilidade INT
                )
                ''')

                cursor.execute(f'''
                CREATE TABLE {self.schema}.pred_estacao (
                    cidade VARCHAR(255) PRIMARY KEY,
                    data DATE DEFAULT CURRENT_DATE,
                    dia DATE DEFAULT DAY(CURRENT_DATE),
                    tempo VARCHAR(255),
                    temp_min FLOAT,
                    temp_max FLOAT,
                    iuv FLOAT
                    FOREIGN KEY (cidade) REFERENCES {self.schema}.cidades(cidade)
                ''')

            self.logger.info(f'Tables created @ {self.dbname} {self.schema}')
            return True
        except Exception as e:
            logger.error(f'Error creating the tables: {e}')
            return False

    def create_upsert_query(self, table: str, columns: List[str]):
        self.logger.info(f'Creating upsert query for @ {table} with {columns}')
        query = f'INSERT INTO {self.schema}.{table} '
        query += ', '.join(columns)
        query += 'VALUES ('
        for item in columns:
            query += '%s, '
        query += ')'
        self.logger.info(f'Query done: {query}')
        return query

    def upsert_data(self, table: str, columns: List[str], values: List[Any]):
        self.logger.info(f'Trying to upsert @ {table} with {columns} of 1 row')
        try:
            query = self.create_upsert_query(table, columns)
            self.logger.info('Starting the upsert')
            with connection.cursor() as cursor:
                cursor.execute(query, data)
            self.logger.info(f'Upsert done @ {table}')
            return True 

        except Exception as e:
            logger.error(f'Error adding item @ {table}: {e}')
            return False

    def upsert_multiple_data(self, table: str, columns: List[str], values: List[List[Any]]):
        self.logger.info(f'Trying to upsert @ {table} with {columns} of {len(values)} rows')
        try:
            query = self.create_upsert_query(table, columns)
            self.logger.info('Starting multiple upserts')
            with connection.cursor() as cursor:
                cursor.executemany(query, values)
            self.logger.info(f'Upsert done @ {table} of {len(values)} rows')
            return True
        except Exception as e:
            self.logger.error(f'Error trying to upsert @ {table} with {columns} of {len(values)} rows')
            return False


    def get_code_cidade(self, cidade = 'Recife'):
        query = f'''
        SELECT id FROM {self.schema}.cidades
        WHERE {self.schema}.cidade = {cidade}
        '''
        with self.get_cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchone()
        return data[0]