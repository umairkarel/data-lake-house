import os
import asyncio
import random

from app.core.order_generator import OrderEventGenerator
from app.providers.kafka.managers.aio_producer_manager import AIOKafkaProducerManager

# ---------------------------------------------------------------------------
# Shared generator instance — persists across HTTP requests AND background loop
# so stats accumulate for the full session lifetime.
# ---------------------------------------------------------------------------
shared_generator = OrderEventGenerator()


async def continuous_generation_task(app):
    is_active = os.environ.get("ACTIVE_GENERATION", "false").lower() == "true"
    if not is_active:
        print("ACTIVE_GENERATION is false. Background continuous generation is disabled.")
        return

    topic_name        = os.environ.get("KAFKA_TOPIC",              "order_events")
    bootstrap_servers = os.environ.get("KAFKA_BROKER",             "kafka:9092")
    interval          = float(os.environ.get("GENERATION_INTERVAL_SEC", "2.0"))

    print(f"Starting continuous order-event generation to topic '{topic_name}' every {interval}s...")

    producer = AIOKafkaProducerManager(
        bootstrap_servers=bootstrap_servers,
        acks=1,
        enable_idempotence=False,
    )

    try:
        while True:
            count  = random.randint(1, 5)
            events = shared_generator.generate_batch(count)

            print(f"Generating {count} order events to '{topic_name}'...")

            for event in events:
                await producer.publish_msg(
                    topic=topic_name,
                    value=event,
                    key=event.get("order_id"),   # partition by order_id for ordering
                )

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        print("Continuous generation task cancelled.")
    except Exception as e:
        print(f"Error in continuous generation task: {e}")


async def start_background_tasks(app):
    app['continuous_generation'] = asyncio.create_task(continuous_generation_task(app))


async def cleanup_background_tasks(app):
    app['continuous_generation'].cancel()
    await app['continuous_generation']
