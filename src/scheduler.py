"""
Local orchestrator: run a whole dag_run in ONE process by starting a dispatcher and
a few workers as coroutines, all coordinating through the Redis queue.

This is a convenience for examples and tests. The SAME dispatcher and worker code
also run as separate processes for a real multi-machine setup:
    python -m src.dispatcher <dag_run_id>
    python -m src.worker w1        # ...and w2, w3, in other terminals

In Phase 1/2 this file held the entire single-process loop (decide + execute). Phase 3
split that into dispatcher.py (decide + enqueue) and worker.py (dequeue + execute);
run_dag just wires them together in-process.
"""

import asyncio

from src import queue
from src.dispatcher import run_dispatcher
from src.worker import run_worker


async def run_dag(
    pool, dag_run_id: int, poll_interval: float = 1.0, n_workers: int = 2
) -> None:
    """Drive one dag_run to completion with an in-process dispatcher + N workers.

    The dispatcher returns once the DAG is finished (every task terminal, so no task
    is queued or being executed). At that point the workers are idle, so we cancel
    their infinite loops and wait for them to unwind.
    """
    workers = [
        asyncio.create_task(run_worker(pool, f"w{i + 1}"))
        for i in range(n_workers)
    ]
    try:
        await run_dispatcher(pool, dag_run_id, poll_interval=poll_interval)
    finally:
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await queue.aclose()
