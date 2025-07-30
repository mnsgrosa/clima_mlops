from prefect import flow, task
from client.api_clima import CPTECApiCaller
import psycopg2
from psycopg2 import sql
from config import DB
import logging

logger = logging.Logger(__name__)

@task
def check_db(connection):
    with connection.cursor() as cursor:
        cursor.execute('SELECT 1 FROM pg_database WHERE datname = clima;')
        return cursor.fetchone() is not None

@task
def create_db(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute('CREATE DATABASE clima')
        return True
    except Exception as e:
        logger.error(f'Error creating the db: {e}')
        return False

@task
def create_schema(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute('''
            CREATE SCHEMA clima_schema 
            ''')
        return True
    except Exception as e:
        logger.erro(f'Error creating schema:{e}')
        return False

@task
def create_tables(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute('''
            CREATE TABLE metar(
                estacao VARCHAR(4) PRIMARY KEY,
                data DATE DEFAULT CURRENT_DATE,
                pressao FLOAT,
                tempo VARCHAR(2),
                temp_desc VARCHAR(255),
                umidade FLOAT,
                vento_dir VARCHAR(255),
                vento_int FLOAT,
                visibilidade INT
            )
            ''')

            cusror.execute('''
            CREATE TABLE pred_estacao (
                cidade VARCHAR(255) PRIMARY KEY,
                data DATE DEFAULT CURRENT_DATE,
                dia DATE,
                tempo VARCHAR(255),
                temp_min FLOAT,
                temp_max FLOAT,
                iuv FLOAT
            )
            ''')
        return True
    except Exception as e:
        logger.error(f'Error creating the table: {e}')
        return False

@task
def get_lattest_data(caller: CPTECApiCaller, connection):
    try:
        tempo_atual = caller.get_condicao_estacao()
        query = '''
            INSERT INTO clima_schema.metar
            '''
        query = query.join(list(tempo_atual.keys()))
        query += 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        data = [item for _, item in tempo_atual.items()]
        with connection.cursor() as cursor:
            cursor.execute(query, data)
        return True
        
    except Exception as e:
        logger.error(f'Error adding items to the table: {e}')
        return False