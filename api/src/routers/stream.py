import asyncio
import json
import logging
import os
from collections import defaultdict

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

log = logging.getLogger("api.stream")

router = APIRouter()


class TradeBroadcaster:
    """Fans incoming trade messages out to per-symbol WebSocket subscribers."""

    def __init__(self, queue_size: int = 100):
        self._queue_size = queue_size
        self._subscribers: dict[str, set[asyncio.Queue[str]]] = defaultdict(set)

    def subscribe(self, symbol: str) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers[symbol].add(queue)
        return queue

    def unsubscribe(self, symbol: str, queue: asyncio.Queue[str]) -> None:
        self._subscribers[symbol].discard(queue)
        if not self._subscribers[symbol]:
            del self._subscribers[symbol]

    def publish(self, symbol: str, message: str) -> None:
        for queue in self._subscribers.get(symbol, ()):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                # Slow client: drop the message rather than stall everyone else.
                pass


async def consume_trades(broadcaster: TradeBroadcaster) -> None:
    """Background task: trades.raw -> broadcaster. Retries until Kafka is up."""
    from aiokafka import AIOKafkaConsumer

    broker = os.environ.get("KAFKA_BROKER", "redpanda:9092")
    topic = os.environ.get("TRADES_TOPIC", "trades.raw")

    while True:
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=broker,
            auto_offset_reset="latest",
            enable_auto_commit=False,
        )
        try:
            await consumer.start()
            log.info("Consuming %s from %s", topic, broker)
            async for msg in consumer:
                payload = msg.value.decode()
                symbol = msg.key.decode() if msg.key else json.loads(payload).get("symbol", "")
                if symbol:
                    broadcaster.publish(symbol, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Kafka consumer failed; retrying in 5s")
            await asyncio.sleep(5)
        finally:
            await consumer.stop()


@router.websocket("/ws/{symbol}")
async def ws_trades(websocket: WebSocket, symbol: str) -> None:
    await websocket.accept()
    broadcaster: TradeBroadcaster = websocket.app.state.broadcaster
    symbol = symbol.upper()
    queue = broadcaster.subscribe(symbol)
    try:
        while True:
            await websocket.send_text(await queue.get())
    except WebSocketDisconnect:
        pass
    finally:
        broadcaster.unsubscribe(symbol, queue)
