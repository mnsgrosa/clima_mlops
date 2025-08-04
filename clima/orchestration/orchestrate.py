from flows import metar_flow, drifting_flow
from api_clima import CPTECApiCaller
import asyncio

async def create_deployments():
    work_pool_name = 'clima-ops'

    metar_result = await metar_flow.afrom_source(
        source = '.',
        entrypoint = 'clima/orchestration/flows.py:metar_flow'
    ).deploy(
        name = 'metar-flow',
        work_pool_name = work_pool_name,
        schedule = {
            'cron':'0 * * * *',
            'timezone':'UTC'
        }
    )

    drifting_flow = await  drifting_flow.afrom_source(
        source = './',
        entrypoint = 'clima/orchestration/flows.py:drifting_flow'
    ).to_deployment(
        name = 'drifting-flow',
        work_pool_name = work_pool_name,
        schedule = {
            'cron':'10 * * * *',
            'timezone':'UTC'
        }
    )

if __name__ == '__main__':
    asyncio.run(create_deployments())