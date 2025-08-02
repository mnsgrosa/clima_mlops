from prefect import flow, task, State
from prefect.states import Failed, Completed
from datetime import datetime
from api_clima import CPTECApiCaller
from math import radians, sin, cos
from scipy.stats import kstest, chisquare
import httpx
from typing import Dict, Any
import pandas as pd

URL = 'http://localhost:8000'

@task
async def get_lattest_metar_api(caller: CPTECApiCaller, estacao:str = 'SBRF') -> Dict[str, Any]:
    return caller.get_condicao_estacao(estacao)

@task
async def metar_etl(data) -> pd.DataFrame:
    if data.empty:
        return data

    temp = data.copy()
    
    temp.drop(columns = ['estacao'], inplace = True)
    
    temp['data'] = pd.to_datetime(temp['data'])
    temp['dia'] = temp['data'].dt.day
    temp['mes'] = temp['data'].dt.month
    temp['ano'] = temp['data'].dt.year
    temp.drop(columns = 'data', inplace = True)

    le = LabelEncoder()
    temp['tempo'] = le.fit_transform(temp['tempo'])
    
    temp.drop(columns = 'tempo_desc', inplace = True)
    
    seno = lambda x: sin(radians(x))
    cosseno = lambda x: cos(radians(x))
    temp['vento_dir_seno'] = temp['vento_dir'].apply(seno)
    temp['vento_dir_cosseno'] = temp['vento_dir'].apply(cosseno)
    temp.drop(columns = 'vento_dir', inplace = True)

    temp['umidade'] /= 100

    return temp

@task
async def post_metar_etl(data) -> State:
    try:
        with httpx.AsyncClient() as client:
            response = await client.post(URL + '/post/etl', json = data)
            response.raise_for_status()
        return Completed(message = 'Metar flow finished succesfully')
    except Exception as e:
        return Failed(message = f'Metar flow failed due to:{e}')

@flow(log_prints  = True)
async def metar_flow(caller):
    data = await get_lattest_metar_api(caller)
    data = await metar_etl(data)
    state = await post_metar_etl(data)
    return state

@task
async def get_lattest_previsoes_api(caller: CPTECApiCaller, cidade:str = 'Recife') -> List[Dict[str, Any]]:
    return caller.get_previsao(cidade)

@task
async def post_lattest_previsao_api(data) -> State:
    try:
        with httpx.AsyncClient() as client:
            response = await client.post(URL + '/post/previsao', json = data)
            response.raise_for_status()
        return Completed(message = 'Previsao flow finished successfully') if response.json()['status'] else Failed(message = 'Failed to post')
    except Exception as e:
        return Failed(message = f'Failed previsao flow due to:{e}')


@flow(log_prints = True)
async def previsao_flow(caller):
    data = await get_lattest_previsoes_api(caller)
    await post_lattest_previsao_api(data)

@task
async def get_metar(estacao: str = 'SBRF') -> pd.DataFrame:
    try:
        with httpx.AsyncClient() as client:
            response = client.get(URL + '/get/metar', params = {'estacao':estacao})
            response.raise_for_status()
        return response.df
    except:
        return pd.DataFrame()

@task
async def drifting(data):
    for col in data.select_dtypes(include = ['float64', 'int64']).columns:
        temp = kstest(data[col].values, random_state = 42).pvalue
        if temp < 0.05:
            return True
    return False

@flow(log_prints = True)
async def drifting_flow():
    data = await get_metar()
    return drifting(data)