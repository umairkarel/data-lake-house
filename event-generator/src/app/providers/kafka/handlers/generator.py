from json import loads

from app.core.asyncio_generator import AsyncioGenerator
from app.core.generator import (
    MessageGenerator,
)
from app.providers.kafka.managers.aio_producer_manager import (
    AIOKafkaProducerManager,
)
from app.responses.response import (
    failed_response,
    success_response,
)
from app.utils.transform import bool_transformator


async def handle_generate(request):
    data = await request.post()

    topic_name = data.get("topic_name")
    bootstrap_servers = data.get("bootstrap_servers")
    schema = loads(data.get("schema", {}))
    count = int(data.get("count", 10))
    unique = (
        False
        if data.get("unique") is None
        else bool_transformator(data.get("unique"))
    )
    parallelism = int(data.get("parallelism", 1))
    time_period = int(data.get("time_period", 1))
    session_window = int(data.get("session_window", 1))
    acks = (lambda x: int(x) if x in ("0", "1") else x)(data.get("acks", "1"))
    enable_idempotence = (
        False
        if data.get("enable_idempotence") is None
        else bool_transformator(data.get("enable_idempotence"))
    )
    topic_key = data.get("topic_key", None)

    msg_generator = MessageGenerator(schema=schema, count=count, unique=unique)
    producer = AIOKafkaProducerManager(
        bootstrap_servers=bootstrap_servers,
        acks=acks,
        enable_idempotence=enable_idempotence,
    )

    log_data = {
        "topic_name": topic_name,
        "bootstrap_servers": bootstrap_servers,
        "count": count,
        "unique": unique,
        "time_period": time_period,
        "session_window": session_window,
        "acks": acks,
        "enable_idempotence": enable_idempotence,
        "topic_key": topic_key,
    }
    try:
        from time import time

        s = time()
        generator = AsyncioGenerator(
            producer=producer,
            message_generator=msg_generator,
            parallelism=parallelism,
            time_period=time_period,
            session_window=session_window,
            topic_name=topic_name,
            topic_key=topic_key,
        )
        await generator.generate()

        print(f"Time: {time() - s}")

    except Exception as exc:
        print(exc)
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)

    log_data.update({"msg": "Message have been published successfully"})
    return success_response(data=log_data)
