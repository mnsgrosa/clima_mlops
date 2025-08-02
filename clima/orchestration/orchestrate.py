from prefect import deploy
from prefect.server.schemas.schedules import CronSchedule
from flows import metar_flow, previsao_flow, drifting_flow
import asyncio

async def create_deployments():
    work_pool_name = 'clima-ops'

    metar = await metar_flow.from_source(
        source = '.',
        entrypoint = 'orchestrate.py:metar_flow'
    ).deploy(
        name = 'metar-flow',
        work_pool_name = work_pool_name,
        work_queue_name = 'metar-queue',
        schedule = CronSchedule(
            cron = '0 * * * *',
            timezone = 'UTC'
        )
    )

    drifiting = await drifting_flow.from_source(
        source = '.',
        entrypoint = 'orchestrate.py:drifting_flow'
    ).deploy(
        name = 'drifting-flow',
        work_pool_name = work_pool_name,
        work_queue_name = 'drifting-queue',
        schedule = CronSchedule(
            cron = '10 * * * *',
            timezone = 'UTC'
        )
    )

    previsao = await previsao_flow.from_source(
        source = '.',
        entrypoint = 'orchestrate.py:previsao_flow'
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