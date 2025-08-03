import httpx
import json
import pandas as pd
import logging
import psycopg2
from typing import List, Dict, Any
from bs4 import BeautifulSoup

class CPTECApiCaller:
    def __init__(self):
        self.url = 'http://servicos.cptec.inpe.br/XML'
        self.cidades_dict = self.scrape_cidades()

    def scrape_cidades(self) -> Dict[str, Dict[str, str]]:
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

    def get_metar(self, estacao = 'SBRF') -> Dict[str, Any]:
        try:
            with httpx.Client() as client:
                response = client.get(self.url + f'/estacao/{estacao}/condicoesAtuais.xml')
                response.raise_for_status()
            
            soup = BeautifulSoup(response.text)
            metar = soup.find('METAR')
    
            tempo = {
                    'estacao': estacao,
                    'data': metar.find('atualizacao').text,
                    'pressao': float(metar.find('pressao').text),
                    'temperatura': float(metar.find('temperatura').text),
                    'tempo':  metar.find('tempo').text,
                    'umidade': float(metar.find('umidade').text),
                    'vento_dir': float(metar.find('vento_dir').text),
                    'vento_int': float(metar.find('vento_int').text),
                    'visibilidade': float(metar.find('visibilidade').text)
            }
            
        except Exception as e:
            tempo = {}
        return tempo

    def get_previsao(self, cidade: str = 'Recife') -> Dict[str, Any]:
        with httpx.Client() as client:
            response = client.get(self.url + f"/cidade/{self.cidades_dict[cidade]['id']}/previsao.xml")
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