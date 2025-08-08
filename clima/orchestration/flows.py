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
from drift_tool import drift_detector
from model.train_class import MyModel

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
        return True
    except Exception as e:
        return False

# first flow to happen -> happens every 2 hours
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
        return data
    else:
        return None


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
async def metar_etl_daily(df, estacao: str = 'SBRF') -> pd.DataFrame:
    data = df.copy()
    colunas = ['temperatura', 'umidade', 'vento_int', 'visibilidade', 'vento_dir_seno', 'vento_dir_cosseno', 'pressao']
    colunas_add = []
    for coluna in colunas:
        data[f'{coluna}_media_dia'] = data[coluna].rolling(window = 24).mean()
        data[f'{coluna}_std_dia'] = data[coluna].rolling(window = 24).std()
        data[f'{coluna}_min_dia'] = data[coluna].rolling(window = 24).min()
        data[f'{coluna}_max_dia'] = data[coluna].rolling(window = 24).max()
        data[f'{coluna}_lag_dia'] = data[coluna].shift(24)

    agg_diario = data['temperatura'].resample('D').agg(['max', 'min'])
    data['target_max'] = agg_diario.max.shift(-24)
    data['target_min'] = agg_diario.min.shift(-24)

    final_data = data.at_time('23:00').copy()
    
    return final_data

@flow
async def process_metar_daily():


@task
async def check_metrics(new_data, old_data):
    return drift_detector(new_data, old_data)

@flow(log_prints = True)
async def check_drift_flow():
    data = await get_metar()
    old_data = data.iloc[:-31]
    new_data = data.iloc[-31:]
    drift = await check_metrics(new_data, old_data)
    return drift, data

@task
async def train_etl(df):
   

@task
async def train(data):
    model = MyModel()
    X_train = data.iloc[:-60].drop(columns = ['target_max', 'target_min'])
    y_train = data.iloc[:-60]
    y_train_max = y_train['target_max']
    y_train_min = y_train['target_min']
    X_eval = data.iloc[-60:-30].drop(columns = ['target_max', 'target_min'])
    y_eval = data.iloc[-60:-30]
    y_eval_max = y_eval['target_max']
    y_eval_min = y_eval['target_min']

    await model.fit(X_train, y_train_max, X_eval, y_eval_max, mode = 'max')
    await model.optimize(X_train, y_train_max, X_eval, y_eval_max, mode = 'max')
    await model.fit(X_train, y_train_min, X_eval, y_eval_min, mode = 'min')
    await model.optimize(X_train, y_eval_min, X_eval, y_eval_min, mode = 'min')
    return model 

@flow(log_prints = True)
def train_flow():
    data = await get_metar()
    data = await train_etl(data)
    model = await train(data)
    return model

@flow(log_prints = True)
async def decision_flow(data):
    if metar_flow():
        drift, data = await check_drift_flow()
        if drift:
            retrain = await retrain_flow()
        pred = await pred_flow()
        
@task
async def get_all_preds():
    async with httpx.AsyncClient() as client:
        response = await client.get(URL + '/get/previsao')
        response.raise_for_status()
    return response.json()
