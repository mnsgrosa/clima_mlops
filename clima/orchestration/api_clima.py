import httpx
import json
import pandas as pd
import logging
import psycopg2
from typing import List, Dict, Any
from bs4 import BeautifulSoup

class CPTECApiCaller:
    def __init__(self, config):
        self.url_base = 'http://servicos.cptec.inpe.br/XML'

    def get_metar(self, estacao = 'SBRF') -> Dict[str, Any]:
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

        for distancia, previsao in enumerate(previsoes):
            previsao_retorno.append({
                'cidade':cidade,
                'data':data,
                'dia_previsao':previsao.find('dia').text,
                'tempo':previsao.find('tempo').text,
                'temp_min':previsao.find('minima').text,
                'temp_max':previsao.find('maxima').text,
                'iuv':previsao.find('iuv').text,
                'diferenca_dias':distancia + 1
                }
            )
        
        return previsao_retorno