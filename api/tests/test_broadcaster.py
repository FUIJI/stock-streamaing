from src.routers.stream import TradeBroadcaster


async def test_publish_reaches_only_matching_symbol():
    b = TradeBroadcaster()
    q_aapl = b.subscribe("AAPL")
    q_msft = b.subscribe("MSFT")

    b.publish("AAPL", '{"symbol":"AAPL"}')

    assert q_aapl.get_nowait() == '{"symbol":"AAPL"}'
    assert q_msft.empty()


async def test_unsubscribe_stops_delivery():
    b = TradeBroadcaster()
    q = b.subscribe("AAPL")
    b.unsubscribe("AAPL", q)
    b.publish("AAPL", "x")
    assert q.empty()


async def test_slow_subscriber_drops_instead_of_blocking():
    b = TradeBroadcaster(queue_size=2)
    q = b.subscribe("AAPL")
    for i in range(5):
        b.publish("AAPL", str(i))
    assert q.qsize() == 2  # extra messages dropped, no exception
