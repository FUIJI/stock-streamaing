import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .db import create_pool
from .routers import candles, stream
from .routers.stream import TradeBroadcaster, consume_trades

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool()
    app.state.broadcaster = TradeBroadcaster()
    consumer_task = asyncio.create_task(consume_trades(app.state.broadcaster))
    yield
    consumer_task.cancel()
    with suppress(asyncio.CancelledError):
        await consumer_task
    await app.state.pool.close()


app = FastAPI(title="Stock Streaming API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(candles.router)
app.include_router(stream.router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
