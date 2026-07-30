"""
Retry policy as pure logic — the same idea as graph.py, applied to "should this
task be retried, and if so, how long should we wait first?"

Kept free of asyncpg and async so the backoff math (easy to get subtly wrong —
off-by-one on the attempt count, or forgetting to cap growth) is unit-testable
without a database. scheduler.py is what wires this decision to db.py.
"""


def plan_retry(
    retry_count: int,
    max_retries: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> float | None:
    """
    Decide what to do after a task's handler raised.

    retry_count: how many retries have ALREADY happened (0 on the first failure).
    max_retries: how many retries are allowed in total.

    Returns the number of seconds to wait before the next attempt, or None if the
    retries are exhausted (meaning: give up and mark the task failed for real).

    Backoff is exponential — 1s, 2s, 4s, ... — so a flapping dependency isn't
    hammered, capped at max_delay so it can't grow without bound.
    """
    if retry_count >= max_retries:
        return None
    return min(base_delay * (2 ** retry_count), max_delay)
