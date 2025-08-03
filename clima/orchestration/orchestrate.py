from prefect import Flow, aserve
from api_clima import CPTECApiCaller
import asyncio

async def create_deployments():
    work_pool_name = 'clima-ops'

    metar_flow = await Flow.afrom_source(
        source = '.',
        entrypoint = 'clima/orchestration/flows.py:metar_flow'
    )

    metar = await metar_flow.to_deployment(
        name = 'metar-flow',
        schedule = {
            'cron':'0 * * * *',
            'timezone':'UTC'
        }
    )

    metar_result = await aserve(metar)

    drifting_flow = await Flow.afrom_source(
        source = './',
        entrypoint = 'clima/orchestration/flows.py:drifting_flow'
    )

    drifting = await drifting_flow.to_deployment(
        name = 'drifting-flow',
        schedule = {
            'cron':'10 * * * *',
            'timezone':'UTC'
        }
    )

    drifting_result = await aserve(drifting)

    previsao_flow = await Flow.afrom_source(
        source = './',
        entrypoint = 'clima/orchestration/flows.py:previsao_flow'
    )

    previsao = await previsao_flow.to_deployment(
        name = 'previsao-flow',
        schedule = {
            'cron':'0 12 * * *',
            'timezone':'UTC'
        }
    )

    previsao_result = await aserve(previsao)

if __name__ == '__main__':
    asyncio.run(create_deployments())