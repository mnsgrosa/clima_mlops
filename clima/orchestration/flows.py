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
async def get_lattest_metar_api(estacao:str = 'SBRF') -> Dict[str, Any]:
    caller = CPTECApiCaller()
    return pd.DataFrame(caller.get_metar(estacao))

@task
async def metar_etl(data) -> pd.DataFrame:
    if data is None:
        return data

    temp = data.copy()

    temp['data'] = pd.to_datetime(temp['data'])
    temp['dia'] = temp['data'].dt.day.astype(int)
    temp['mes'] = temp['data'].dt.month.astype(int)
    temp['ano'] = temp['data'].dt.year.astype(int)
    temp.drop(columns = 'data', inplace = True)

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

    return temp

@task
async def post_metar_etl(data) -> State:
    if data is None:
        return Failed(message = 'No data to post')

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(URL + '/post/metar', json = data.to_dict())
            response.raise_for_status()
        return Completed(message = 'Metar flow finished succesfully')
    except Exception as e:
        return Failed(message = f'Metar flow failed due to:{e}')

@flow(log_prints  = True)
async def metar_flow():
    data = await get_lattest_metar_api()
    if not data.empty:
        data = await metar_etl(data)
        state = await post_metar_etl(data)
        return state
    else:
        return Failed(message = 'No data to post')
@task
async def get_lattest_previsoes_api(cidade:str = 'Recife') -> List[Dict[str, Any]]:
    caller = CPTECApiCaller()
    return caller.get_metar(cidade)

@task
async def post_lattest_previsao_api(data) -> State:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(URL + '/post/previsao', json = data)
            response.raise_for_status()
        return Completed(message = 'Previsao flow finished successfully') if response.json()['status'] else Failed(message = 'Failed to post')
    except Exception as e:
        return Failed(message = f'Failed previsao flow due to:{e}')


@flow(log_prints = True)
async def previsao_flow():
    data = await get_lattest_previsoes_api()
    result = await post_lattest_previsao_api(data)
    return result

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
            return True
    return False

@flow(log_prints = True)
async def drifting_flow():
    data = await get_metar()
    result = await drifting(data)
    return result