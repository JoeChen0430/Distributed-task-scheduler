"""
A worker process for the multi-process demo.

The ONLY difference from `python -m src.worker` is the `import examples.demo_tasks`
line below: that's what registers the demo handlers in THIS process, so this worker
can actually execute demo_extract / demo_a / demo_b / demo_join. A bare
`python -m src.worker` has an empty registry and would fail on those task types —
because task code isn't shipped over Redis, every worker must import it locally.

    python -m examples.run_worker w1
"""

import asyncio
import sys

from src import db, queue
from src.worker import run_worker
import examples.demo_tasks  # noqa: F401  — registers the demo handlers in this process


async def main() -> None:
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker"
    pool = await db.get_pool()
    print(f"[worker {worker_id}] waiting for tasks...")
    try:
        await run_worker(pool, worker_id)
    finally:
        await queue.aclose()


if __name__ == "__main__":
    asyncio.run(main())
