from prefect import flow, task
from prefect.states import Failed, Completed
from datetime import datetime
import httpx

@task
def get_lattest_metar_data(caller: CPTECApiCaller):
    return caller.get_condicao_estacao()
    
@task
def add_lattest_metar_data(caller: CPTECApiCaller, data):
    # httpx logic here
    pass

@task
def get_lattest_previsoes_data(caller: CPTECApiCaller, cidade:str = 'Recife'):
    return caller.get_previsao(cidade)

@task
def add_lattest_previsao_data(caller: CPTECApiCaller, data):
    if data is None:
        caller.logger.error(f'No previsao data received: {datetime.now()}')
        return False

    caller.logger.info(f'Start of upsert of previsoes data at: {datetime.now()}')
    try:
        caller.upsert_multiple_data('pred_estacao', list(data.keys()), list(data.values))
        caller.logger.info(f'Done of upsert of previsoes data at: {datetime.now()}')
        return True
    except Exception as e:
        caller.logger.error(f'Error of upsert of previsoes data at: {datetime.now()}: {e}')
        return False

@flow(log_prints  = True)
def caller_creation_flow():
    return init_class()

@flow(log_prints  = True)
def metar_flow(caller):
    caller.logger.info('Starting metar flow')
    data = get_lattest_metar_data(caller)
    caller.logger.info(f'Got the data: {len(data)}')
    if add_lattest_metar_data(caller, data):
        return Completed(message = 'New metar data added')
    return Failed(message = 'Failed to add new metar data')

@flow(log_prints = True)
def previsao_flow(caller):
    caller.logger.info('Starting previsao flow')
    data = get_lattest_previsoes_data(caller)
    caller.logger.info(f'Got the data: {len(data)}')
    if add_lattest_previsao_data(caller, data):
        return Completed(message = 'New previsao data added')
    return Failed(message = 'Failed to add new previsao data')

if __name__ == '__main__':
    caller = caller_creation_flow()
    metar_flow(caller)
    previsao_flow(caller)
