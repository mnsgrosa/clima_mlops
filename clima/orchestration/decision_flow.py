from prefect import flow, task, State
from prefect.states import Failed, Completed
from datetime import datetime
from api_clima import CPTECApiCaller
from math import radians, sin, cos
from scipy.stats import kstest, chisquare
import httpx
from typing import Dict, List, Any
import pandas as pd
from sklearn.preprocessing import LabelEncoder

URL = 'http://localhost:8000'

@task
async def get_caller(previsao:bool = False, cidade:str = None) -> Dict[str, Any]:
    caller = CPTECApiCaller(previsao, cidade)
    return caller

@task
async def metar_etl(data: pd.DataFrame) -> pd.DataFrame:
    if data is None:
        return data

    temp = data.copy()

    temp['atualizado_em'] = pd.to_datetime(temp['atualizado_em'])
    temp['dia'] = temp['atualizado_em'].dt.day.astype(int)
    temp['mes'] = temp['atualizado_em'].dt.month.astype(int)
    temp['ano'] = temp['atualizado_em'].dt.year.astype(int)
    temp.drop(columns = 'atualizado_em', inplace = True)

    le = LabelEncoder()
    temp['tempo'] = le.fit_transform(temp['tempo'])
    temp['tempo'] = temp['tempo'].astype(int)

    seno = lambda x: sin(radians(x))
    cosseno = lambda x: cos(radians(x))
    temp['vento_dir_seno'] = temp['vento_dir'].apply(seno).astype(float)
    temp['vento_dir_cosseno'] = temp['vento_dir'].apply(cosseno).astype(float)
    temp.drop(columns = 'vento_dir', inplace = True)

    temp['umidade'] /= 100
    temp['umidade'] = temp['umidade'].astype(float)

    temp['visibilidade'] = temp['visibilidade'].astype(float) / 1000

    return temp

@task
async def post_metar_etl(data) -> State:
    if data.empty:
        return Failed(message = 'No data to post')

    try:
        temp = data.to_dict(orient = 'records')
        async with httpx.AsyncClient() as client:
            response = await client.post(URL + '/post/metar', json = temp)
            response.raise_for_status()
        return Completed(message = 'Metar flow finished succesfully')
    except Exception as e:
        return Failed(message = f'Metar flow failed due to:{e}')

@flow(log_prints  = True)
async def metar_flow():
    caller = await get_caller()
    data = pd.DataFrame(caller.metar, columns = [
        'estacao', 'atualizado_em', 'pressao', 'temperatura', 
        'tempo', 'umidade', 'vento_dir', 'vento_int', 'visibilidade'
    ])
    if not data.empty:
        data = await metar_etl(data)
        state = await post_metar_etl(data)
        return state
    else:
        return Failed(message = 'No data to post')

@task
async def post_lattest_previsao_api(data) -> State:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(URL + '/post/previsao', json = data)
            response.raise_for_status()
        return True if response.json()['status'] else False
    except Exception as e:
        return False


@flow(log_prints = True)
async def previsao_flow():
    caller = await get_caller(previsao = True, cidade = 'Recife')
    result = await post_lattest_previsao_api(caller.previsao)
    return result

@task
async def get_all_preds():
    async with httpx.AsyncClient() as client:
        response = await client.get(URL + '/get/previsao')
        response.raise_for_status()
    return response.df

@task
async def get_dist():
    pass

@task
async def post_dist():
    pass


@flow(log_prints = True)
async def check_metrics():
    pass

@task
async def get_metar(estacao: str = 'SBRF') -> pd.DataFrame:
    try:
        async with httpx.AsyncClient() as client:
            response = client.get(URL + '/get/metar', params = {'estacao':estacao})
            response.raise_for_status()
        return response.df
    except:
        return pd.DataFrame()

@task
async def drifting(data):
    for col in data.select_dtypes(include = ['float64', 'int64']).columns:
        temp = kstest(data[col].values, 'norm', random_state = 42).pvalue
        if temp < 0.05:
            return Completed
    return False

@flow(log_prints = True)
async def drifting_flow():
    data = await get_metar()
    result = await drifting(data)
    return result

@flow(log_prints = True)
async def decision_flow():
    pass
