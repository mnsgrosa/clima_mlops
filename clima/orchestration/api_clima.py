import httpx
import json
import pandas as pd
import logging
import psycopg2
from typing import List, Dict, Any

class CPTECApiCaller:
    def __init__(self, previsao:bool = False, cidade:str = None):
        self.url = 'https://brasilapi.com.br/api/cptec/v1/'
        self.capitais = {
            "Acre": "Rio Branco",
            "Alagoas": "Maceió",
            "Amapá": "Macapá",
            "Amazonas": "Manaus",
            "Bahia": "Salvador",
            "Ceará": "Fortaleza",
            "Distrito Federal": "Brasília",
            "Espírito Santo": "Vitória",
            "Goiás": "Goiânia",
            "Maranhão": "São Luís",
            "Mato Grosso": "Cuiabá",
            "Mato Grosso do Sul": "Campo Grande",
            "Minas Gerais": "Belo Horizonte",
            "Pará": "Belém",
            "Paraíba": "João Pessoa",
            "Paraná": "Curitiba",
            "Pernambuco": "Recife",
            "Piauí": "Teresina",
            "Rio de Janeiro": "Rio de Janeiro",
            "Rio Grande do Norte": "Natal",
            "Rio Grande do Sul": "Porto Alegre",
            "Rondônia": "Porto Velho",
            "Roraima": "Boa Vista",
            "Santa Catarina": "Florianópolis",
            "São Paulo": "São Paulo",
            "Sergipe": "Aracaju",
            "Tocantins": "Palmas"
        }
        self.metars_raw = self.get_metar()
        self.raw = self.get_raw_data()
        self.cidade_codes = {data.get('cidade'):data.get('id') for data in self.raw}
        self.codes = self.get_estacoes()
        self.cidade_estado = {data.get('cidade'):data.get('estado') for data in self.raw}
        self.estados_codes = get_estados_codes()
        if previsao:
            self.previsao = self.get_previsao(cidade)
        else:
            self.metar = self.get_clima_capitais()

    def get_metar(self):
        try:
            with httpx.Client() as client:
                response = client.get(self.url + '/clima/capital')
                response.raise_for_status()
            metars = response.json()
            return metars
        except:
            return []

    def get_estacoes_capitais(self):
        try:
            estacoes_code = []

            for metar in self.metars:
                estacoes_code.append(metar.get('codigo_icao'))
            
            return estacoes_code
        except Exception as e:
            return []
            
    def get_raw_data(self):
        try:
            with httpx.Client() as client:
                response = client.get(self.url + '/cidade')
                response.raise_for_status()
            raw = response.json()
            return raw
        except:
            return []

    def get_clima_capitais(self) -> List[Dict[str, Any]]:
        try:
            tempo = []
            for metar in self.metars_raw:
                tempo.append([
                        metar.get('codigo_icao'),
                        metar.get('atualizado_em'),
                        metar.get('pressao_atmosferica'),
                        metar.get('temp'),
                        metar.get('condicao'),
                        metar.get('umidade'),
                        metar.get('direcao_vento'),
                        metar.get('vento'),
                        metar.get('visibilidade')
                ])
        except Exception as e:
            tempo = []
        return tempo

    def get_previsao(self, cidade: str = 'Recife') -> Dict[str, Any]:
        with httpx.Client() as client:
            response = client.get(self.url + f"/clima/previsao/{self.cidade_codes[cidade]}")
            response.raise_for_status()
        
        data = response.json()
        estado = data.get('estado')
        atualizacao = data.get('atualizado_em')
        previsoes = data.get('clima')

        retorno = []

        for previsao in previsoes:
            retorno.append({
                'cidade':cidade,
                'estado':estado,
                'atualizacao':atualizacao,
                'data':previsao.get('data'),
                'temp_min':previsao.get('min'),
                'temp_max':previsao.get('max'),
                'indice_uv':previsao.get('indice_uv'),
            }
        )

        return retorno