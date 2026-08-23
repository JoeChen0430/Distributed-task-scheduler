"""
Create the distributed-demo DAG and print its id — but do NOT run it. Use this with
standalone workers and a dispatcher to see a DAG execute across separate processes:

    python -m examples.create_demo_dag        # prints dag_run id=N
    python -m examples.run_worker w1          # in another terminal
    python -m examples.run_worker w2          # ...and another
    python -m src.dispatcher N                # drives dag_run N
"""

import asyncio

from src import db
from src.dag import create_dag
from examples.demo_tasks import DEMO_TASK_DEFS


async def main() -> None:
    pool = await db.get_pool()
    dag_run_id = await create_dag(pool, "distributed_demo", DEMO_TASK_DEFS)
    print(f"Created dag_run id={dag_run_id}")
    print(f"Start workers, then:  python -m src.dispatcher {dag_run_id}")


if __name__ == "__main__":
    asyncio.run(main())
