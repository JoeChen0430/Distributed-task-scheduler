import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://scheduler:scheduler@localhost:5432/scheduler",
)

# Phase 3: Redis is the work queue (dispatcher LPUSHes ready task ids, workers
# BRPOP them). Not the source of truth — that's still Postgres.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
