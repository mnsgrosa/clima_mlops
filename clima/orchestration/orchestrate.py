from prefect import deploy
from prefect.server.schemas.schedules import CronSchedule
from flows import metar_flow, previsao_flow, decision_flow, train_min_flow, train_max_flow

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

    decision = await decision_flow.from_source(
        source = '.',
        entrypoint = 'orchestrate.py:decision_flow'
    ).deploy(
        name = 'decision-flow',
        work_pool_name = work_pool_name,
        work_queue_name = 'decision-queue',
        schedule = CronSchedule(
            cron = '0 0 * * *',
            timezone = 'UTC'
        )
    )

    if decision:
        train_min = await train_min_flow.from_source(
            source = '.',
            entrypoint = 'orchestrate.py:train_min_flow'
        ).deploy(
            name = 'train-min-flow',
            work_pool_name = work_pool_name,
            work_queue_name = 'train-min-queue',
            schedule = CronSchedule(
                cron = '0 0 * * *',
                timezone = 'UTC'
            )
        )
        
        train_max_flow = await train_max_flow.from_source(
            source = '.',
            entrypoint = 'orchestrate.py:train_max_flow'
        ).deploy(
            name = 'train-max-flow',
            work_pool_name = work_pool_name,
            work_queue_name = 'train-max-queue',
            schedule = CronSchedule(
                cron = '0 0 * * *',
                timezone = 'UTC'
            )
        )

if __name__ == '__main__':
    asyncio.run(create_deployments())