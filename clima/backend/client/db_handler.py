import psycopg2
import logging
import pandas as pd
from contextlib import contextmanager
from typing import List, Dict, Any

class DBHandler:
    def __init__(self, config: Dict[str, str], dbname:str = 'clima', schema:str = 'clima_schema'):
        self.config = config
        self.logger = self.init_logger()
        self.dbname = dbname
        self.schema = schema
        self.create_db()
        self.create_schema()
        self.create_tables()
        self.dfs = {}

        self.columns_metar = ['estacao', 'data', 'pressao', 'temperatura', 'tempo', 'tempo_desc', 'umidade', 'vento_dir', 'vento_int', 'visibilidade']
        self.columns_pred = ['cidade', 'data', 'dia', 'tempo', 'maxima', 'minima', 'iuv']
        self.columns_cidades = ['cidade', 'uf', 'id']
    
    def init_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG) 
        file_handler = logging.FileHandler('app.log')
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter) 
        logger.addHandler(file_handler)
        
        return logger

    @contextmanager
    def get_cursor(self):
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**self.config)
            cursor = connection.cursor()
            yield cursor
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            self.logger.error(f'Error while using the cursor: {e}')
            raise  
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def upsert_data(self, table = None, columns: List[str] = [], value = None):
        if value is None or table is None or columns is None:
            return False

        try:
            self.logger.info(f'Starting the insertion @ {table} with {columns} of 1 row')
            query = self.create_upsert_query(table, columns)
            with self.get_cursor() as cursor:
                cursor.execute(query, value)
            self.logger.info(f'Done the insertion of {value}')
            return True
        except Exception as e:
            self.logger.error(f'Error inserting @ {table} with {columns}: {e}')
            return False

    def get_data(self, table: str = 'metar', columns: List[str] = [], restriction: Dict[str, str] = {}):
        try:
            self.logger.info(f'Getting data from {columns} from {table}')
            if columns is None:
                query = f"SELECT * FROM {self.schema}.{table}"
            else:
                query = f"SELECT {' '.join(columns)} FROM {self.schema}.{table}"
            
            if value is not None:
                query += f' WHERE'
                for column, value in value.items():
                    query += f' {column} = {value}'

            with self.get_cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
            self.logger.info(f'Got {len(data)} rows from {table}')
            columns = list(data[0].keys())
            data = pd.DataFrame(columns = columns, data = data)
            self.dfs[table] = data
            return data
        except Exception as e:
            self.logger.error(f'Error getting data from {table}: {e}')
            return None

    def create_db(self):
        try:
            self.logging.info(f'Creating database {self.dbname}')
            with self.get_cursor() as cursor:
                cursor.execute(f'CREATE DATABASE {self.dbname}')
            self.logging.info(f'Database {self.dbname} created')
            return True
        except Exception as e:
            self.logger.error(f'Error creating the db: {e}')
            return False

    def create_schema(self):
        try:
            self.logging.info(f'Creating schema {self.schema}')
            with self.get_cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA {self.schema}')
            self.logging.info(f'Schema {self.schema} created')
            return True
        except Exception as e:
            self.logger.error(f'Error creating the schema: {e}')
            return False

    def create_tables(self):
        try:
            self.logging.info(f'creating tables')
            with self.get_cursor() as cursor:
                cursor.execute(f'''
                CREATE TABLE {self.schema}.metar(
                    estacao VARCHAR(4) PRIMARY KEY,
                    dia INT,
                    mes INT,
                    ano INT,
                    pressao FLOAT,
                    temperatura FLOAT,
                    tempo VARCHAR(10),
                    umidade Float,
                    vento_dir_seno FLOAT,
                    vento_dir_cosseno FLOAT,
                    vento_int FLOAT,
                    visibilidade FLOAT
                )
                ''')

                cursor.execute(f'''
                CREATE TABLE {self.schema}.pred_cidade (
                    cidade VARCHAR(255) PRIMARY KEY,
                    estado VARCHAR(255),
                    atualizacao DATE DEFAULT CURRENT_DATE,
                    data DATE NOT NULL,
                    temp_min FLOAT,
                    temp_max FLOAT,
                    indice_uv FLOAT
                ''')

            self.logger.info(f'Tables created @ {self.dbname} {self.schema}')
            return True
        except Exception as e:
            self.logger.error(f'Error creating the tables: {e}')
            return False

    def create_upsert_query(self, table: str, columns: List[str]):
        self.logger.info(f'Creating upsert query for @ {table} with {columns}')
        query = f'INSERT INTO {self.schema}.{table} '
        query += ', '.join(columns)
        query += 'VALUES ('
        for item in columns:
            query += f'%{item}, '
        query += ')'
        self.logger.info(f'Query done: {query}')
        return query

    def upsert_multiple_data(self, table: str = None, columns: List[str] = [], values: List[List[Any]] = []):
        if table is None or columns is None or values is None:
            self.logger.error('Invalid arguments')
            return False
    
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