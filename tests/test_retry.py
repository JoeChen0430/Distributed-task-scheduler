"""
Pure tests for the retry policy — no asyncpg, no database, same spirit as
test_graph.py. Run with:
    python -m pytest tests/
"""

from src.retry import plan_retry


def test_no_retries_configured_gives_up_immediately():
    # max_retries=0 (the default) -> fail on first failure, no retry.
    assert plan_retry(retry_count=0, max_retries=0) is None


def test_first_retry_waits_base_delay():
    # retry_count=0 -> base_delay * 2**0 = base_delay
    assert plan_retry(retry_count=0, max_retries=3, base_delay=1.0) == 1.0


def test_backoff_grows_exponentially():
    assert plan_retry(retry_count=1, max_retries=3, base_delay=1.0) == 2.0
    assert plan_retry(retry_count=2, max_retries=3, base_delay=1.0) == 4.0


def test_retries_exhausted_returns_none():
    # Allowed 2 retries; after the 2nd (retry_count=2) there are none left.
    assert plan_retry(retry_count=2, max_retries=2) is None


def test_delay_is_capped_at_max_delay():
    # 1.0 * 2**10 = 1024s, but capped.
    assert plan_retry(retry_count=10, max_retries=20, base_delay=1.0, max_delay=60.0) == 60.0
