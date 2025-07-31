from prefect.states import Failed, Completed
from datetime import datetime
from api_clima import CPTECApiCaller
import httpx

URL = 'http://localhost:8000'

@task
async def get_lattest_metar_api(caller: CPTECApiCaller, estacao:str = 'SBRF'):
    return caller.get_condicao_estacao(estacao)
    
@task
async def post_lattest_metar_data(data: Dict[str, Any]):
    with httpx.Client() as client:
        response = client.post(URL + '/post/clima', json = data)
        response.raise_for_status()
    return Completed(message = 'Posted successfully') if respose['status'] else Failed(message = 'Failed to post')

@task
async def get_lattest_previsoes_api(caller: CPTECApiCaller, cidade:str = 'Recife'):
    return caller.get_previsao(cidade)

@task
async def post_lattest_previsao_api(data):
    with httpx.Client() as client:
        response = client.post(URL + '/post/previsao', json = data)
        response.raise_for_status()
    return Completed(message = 'Posted successfully') if response['status'] else Failed(message = 'Failed to post')

@flow(log_prints  = True)
async def metar_flow(caller):
    data = get_lattest_metar_api(caller)
    post_lattest_metar_data(data)

@flow(log_prints = True)
async def previsao_flow(caller):
    data = get_lattest_previsoes_api(caller)
    post_lattest_previsao_api(data)

@flow(log_prints = True)
async def decision_flow():
    pass

@flow(log_prints = True)
async def train_min_flow():
    pass

@flow(log_prints = True)
async def train_max_flow():
    pass