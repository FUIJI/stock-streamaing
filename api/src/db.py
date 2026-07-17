import os

import asyncpg
from fastapi import Request


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=os.environ.get("PG_HOST", "timescaledb"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DB", "stocks"),
        user=os.environ.get("PG_USER", "stock"),
        password=os.environ.get("PG_PASSWORD", "stock_dev_password"),
        min_size=1,
        max_size=10,
    )


async def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool
