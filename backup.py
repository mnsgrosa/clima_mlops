from prefect import Flow
from prefect.server.schemas.schedules import CronSchedule
from flows import metar_flow, drifting_flow, previsao_flow
from api_clima import CPTECApiCaller
import asyncio

async def create_deployments():
    work_pool_name = 'clima-ops'
    caller = CPTECApiCaller()

    metar_flow = await Flow.afrom_source(
        source = '.',
        entrypoint = 'clima/orchestration/flows.py:metar_flow'
    )

    metar_deployment = await metar_flow.deploy(
        name = 'metar-flow-deployment',
        work_pool_name = work_pool_name,
        work_queue_name = 'metar-queue',
        parameters = {'caller': caller},
        schedule = CronSchedule(
            cron = '0 * * * *',
            timezone = 'UTC'
        )
    )

    metar = await metar_flow.afrom_source(
        source = './',
        entrypoint = 'clima/orchestration/flows.py:metar_flow'
    ).deploy(
        name = 'metar-flow',
        work_pool_name = work_pool_name,
        work_queue_name = 'metar-queue',
        parameters = {'caller': caller},
        schedule = CronSchedule(
            cron = '0 * * * *',
            timezone = 'UTC'
        )
    )

    drifiting = await drifting_flow.afrom_source(
        source = './',
        entrypoint = 'clima/orchestration/flows.py:drifting_flow'
    ).deploy(
        name = 'drifting-flow',
        work_pool_name = work_pool_name,
        work_queue_name = 'drifting-queue',
        schedule = CronSchedule(
            cron = '10 * * * *',
            timezone = 'UTC'
        )
    )

    previsao = await Flow.afrom_source(
        source = './',
        entrypoint = 'clima/orchestration/flows.py:previsao_flow'
    ).deploy(
        name = 'previsao-flow',
        work_pool_name = work_pool_name,
        work_queue_name = 'previsao-queue',
        schedule = CronSchedule(
            cron = '0 12 * * *',
            timezone = 'UTC'
        )
    )

if __name__ == '__main__':
    asyncio.run(create_deployments())