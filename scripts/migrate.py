"""
Applies migrations/001_init_schema.sql using the same asyncpg connection the
rest of the app uses, so you don't need the `psql` CLI installed locally.

Run from the project root:
    python -m scripts.migrate
"""

import asyncio
from pathlib import Path

from src import db

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


async def main() -> None:
    pool = await db.get_pool()
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not migration_files:
        print(f"No .sql files found in {MIGRATIONS_DIR}")
        return

    async with pool.acquire() as conn:
        for path in migration_files:
            print(f"Applying {path.name} ...")
            sql = path.read_text()
            await conn.execute(sql)

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
