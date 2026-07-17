import asyncio
import logging

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import Settings
from .sources import build_source

log = logging.getLogger("ingestion")


def ensure_topic(broker: str, topic: str, partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions)])
    try:
        futures[topic].result(timeout=10)
        log.info("Created topic %s (%d partitions)", topic, partitions)
    except KafkaException as exc:
        # Racing another instance is fine; anything else is not.
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise


def _on_delivery(err, msg) -> None:
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)


async def run() -> None:
    settings = Settings.from_env()
    log.info(
        "Starting ingestion: source=%s symbols=%s topic=%s",
        settings.trade_source, settings.symbols, settings.trades_topic,
    )
    ensure_topic(settings.kafka_broker, settings.trades_topic)

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_broker,
            "linger.ms": 50,
            "acks": "all",
        }
    )
    source = build_source(
        settings.trade_source, settings.symbols,
        settings.sim_trades_per_sec, settings.finnhub_api_key,
    )

    produced = 0
    try:
        async for trade in source.stream():
            producer.produce(
                settings.trades_topic,
                key=trade.symbol.encode(),
                value=trade.model_dump_json().encode(),
                on_delivery=_on_delivery,
            )
            producer.poll(0)
            produced += 1
            if produced % 500 == 0:
                log.info("Produced %d trades (last: %s @ %s)", produced, trade.symbol, trade.price)
    finally:
        log.info("Flushing producer (%d trades produced)", produced)
        producer.flush(10)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
