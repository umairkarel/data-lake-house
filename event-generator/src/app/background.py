import os
import asyncio
from json import dumps
import random

from app.core.asyncio_generator import AsyncioGenerator
from app.core.generator import MessageGenerator
from app.providers.kafka.managers.aio_producer_manager import AIOKafkaProducerManager

async def continuous_generation_task(app):
    is_active = os.environ.get("ACTIVE_GENERATION", "false").lower() == "true"
    if not is_active:
        print("ACTIVE_GENERATION is false. Background continuous generation is disabled.")
        return

    topic_name = os.environ.get("KAFKA_TOPIC", "benchmark_events")
    bootstrap_servers = os.environ.get("KAFKA_BROKER", "kafka:9092")
    interval = float(os.environ.get("GENERATION_INTERVAL_SEC", "2.0"))
    
    # We use the schema that matches the lakehouse table: 
    # id=INTEGER, value=INTEGER, amount=FLOAT, event_time=DATETIME, ingestion_time=DATETIME
    schema = {
        "id": "INTEGER",
        "value": "INTEGER", 
        "amount": "FLOAT",
        "event_time": "DATETIME",
        "ingestion_time": "DATETIME"
    }

    print(f"Starting continuous generation to topic '{topic_name}' every {interval} seconds...")

    producer = AIOKafkaProducerManager(
        bootstrap_servers=bootstrap_servers,
        acks=1,
        enable_idempotence=False,
    )

    try:
        while True:
            # We will generate a small batch of 1 to 5 records each time
            count = random.randint(1, 5)
            msg_generator = MessageGenerator(schema=schema, count=count, unique=True)
            
            generator = AsyncioGenerator(
                producer=producer,
                message_generator=msg_generator,
                parallelism=1,
                time_period=1,
                session_window=1,
                topic_name=topic_name,
                topic_key=None,
            )
            
            print(f"Generating {count} background events to '{topic_name}'...")
            await generator.generate()
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
