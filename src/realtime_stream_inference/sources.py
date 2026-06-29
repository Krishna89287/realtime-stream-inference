from __future__ import annotations

import asyncio
from typing import AsyncIterator, Iterable

from .models import Event


async def iterable_source(events: Iterable[Event], delay: float = 0.0) -> AsyncIterator[Event]:
    """Wrap an in-memory iterable as an async stream. Handy for tests and demos."""
    for event in events:
        if delay:
            await asyncio.sleep(delay)
        yield event


# Kafka adapter sketch. Kept out of the default path so the project runs with no
# broker. Install confluent-kafka and point RSI_KAFKA_* at your cluster to use it.
#
# async def kafka_source(topic: str, brokers: str) -> AsyncIterator[Event]:
#     from confluent_kafka import Consumer
#
#     consumer = Consumer(
#         {"bootstrap.servers": brokers, "group.id": "rsi", "auto.offset.reset": "latest"}
#     )
#     consumer.subscribe([topic])
#     loop = asyncio.get_running_loop()
#     try:
#         while True:
#             msg = await loop.run_in_executor(None, consumer.poll, 1.0)
#             if msg is None or msg.error():
#                 continue
#             yield Event.model_validate_json(msg.value())
#     finally:
#         consumer.close()
