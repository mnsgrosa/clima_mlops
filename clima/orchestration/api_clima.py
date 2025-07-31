import httpx
import json
import pandas as pd
import logging
import psycopg2
from config import DB
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from db_handler import DBHandler

class CPTECApiCaller:
    def __init__(self, config):
        self.url_base = 'http://servicos.cptec.inpe.br/XML'
        self.config = db
        self.cidades_dict = self.scrape_cidades()
        self.condicoes_estacoes = {}
        self.previsao_temperatura = {}
        self.iuvs = {}

    def scrape_cidades(self) -> List[Dict[str, str]]:
        with httpx.Client() as client:
            response = client.get(self.url + '/listaCidades')
            response.raise_for_status()

        data = response.text
        soup = BeautifulSoup(data, 'xml')
        cidades = soup.find('cidades').find_all('cidade')

        cidades_dict = {}

        for cidade in cidades:
            cidades_dict[cidade.find('nome').text] = {'uf':cidade.find('uf').text, 'id':cidade.find('id').text}
        
        return cidades_dict

    def get_metar(self, estacao = 'SBRF'):
        self.logger.info('Getting weather conditions')
        try:
            self.logger.info('Starting the request')
            with httpx.Client() as client:
                response = client.get(self.url + f'/estacao/{estacao}/condicoesAtuais.xml')
                response.raise_for_status()
            
            self.logger.info('Request done')
            soup = BeautifulSoup(response.text)
            metar = soup.find('METAR')
    
            tempo = {
                    'estacao': estacao,
                    'data': metar.find('atualizacao').text,
                    'pressao': metar.find('pressao').text,
                    'temperatura': metar.find('temperatura').text,
                    'tempo': metar.find('tempo').text,
                    'tempo_desc': metar.find('tempo_desc').text,
                    'umidade': metar.find('umidade').text,
                    'vento_dir': metar.find('vento_dir').text,
                    'vento_int': metar.find('vento_int').text,
                    'visibilidade': metar.find('visibilidade').text
            }

            if estacao not in self.condicoes_estacoes:
                self.condicoes_estacoes[estacao] = [tempo]
            else:
                self.condicoes_estacoes[estacao].append(tempo)
            self.logger.info('Got the info')
        except Exception as e:
            self.logger.error(f'Error getting the info: {e}')
            tempo = []
        return tempo

    def get_previsao(self, cidade: str = 'Recife') -> Dict[str, Any]:
        with httpx.Client() as client:
            response = client.get(self.url + f"/cidade/{self.get_code_cidade(cidade)}/previsao.xml")
            response.raise_for_status()
        
        soup = BeautifulSoup(response.text)
        data = soup.find('atualizacao')
        
        previsoes = soup.find_all('previsao')
        previsoes_dict = {}
        previsao_retorno = []

        for previsao in previsoes:
            previsoes_dict['data'] = (
                previsao.find('dia').text,
                previsao.find('tempo').text,
                previsao.find('maxima').text,
                previsao.find('minima').text,
                previsao.find('iuv').text
            )
            previsao_retorno.append([
                cidade,
                data,
                previsao.find('dia').text,
                previsao.find('tempo').text,
                previsao.find('maxima').text,
                previsao.find('minima').text,
                previsao.find('iuv').text 
            ])


        previsao_hoje = {data:previsoes_dict}

        if cidade not in self.previsao_temperatura:
            self.previsoes[cidade] = [previsao_hoje]
        else:
            self.previsoes[cidade].append(previsao_hoje)

        return previsao_retorno
    
    def get_iuv(self, cidade: str = 'Recife') -> Dict[str, Any]:
        cidade.capitalize()
        with httpx.Client() as client:
            response = client.get(self.url + f"/cidade/{self.get_code_cidade(cidade)}/dia/0/ondas.xml")
            response.raise_for_status()

        soup = BeautifulSoup(response.text)
        data = {
                'data':soup.find('data').text,
                'hora':soup.find('hora').text,
                'iuv':soup.find('iuv').text
        }
        
        if cidade not in self.iuvs:
            self.iuvs[cidade] = [data]
        else:
            self.iuvs[cidade].append(data)
        
        return data