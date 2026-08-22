"""
REST handler for POST /kafka/generateMessages

Triggers a one-shot batch of order_events via the REST API.
Uses OrderEventGenerator (same as background loop) so the schema
is always consistent.
"""
from app.core.order_generator import OrderEventGenerator
from app.providers.kafka.managers.aio_producer_manager import AIOKafkaProducerManager
from app.responses.response import failed_response, success_response


async def handle_generate(request):
    data = await request.post()

    topic_name        = data.get("topic_name", "order_events")
    bootstrap_servers = data.get("bootstrap_servers", "kafka:9092")
    count             = int(data.get("count", 10))
    acks              = (lambda x: int(x) if x in ("0", "1") else x)(data.get("acks", "1"))

    log_data = {
        "topic_name":        topic_name,
        "bootstrap_servers": bootstrap_servers,
        "count":             count,
    }

    try:
        from time import time
        s = time()

        generator = OrderEventGenerator()
        events    = generator.generate_batch(count)

        producer = AIOKafkaProducerManager(
            bootstrap_servers=bootstrap_servers,
            acks=acks,
            enable_idempotence=False,
        )

        for event in events:
            await producer.publish_msg(
                topic=topic_name,
                value=event,
                key=event.get("order_id"),
            )

        log_data.update({
            "msg":          "Messages published successfully",
            "elapsed_sec":  round(time() - s, 3),
        })
        return success_response(data=log_data)

    except Exception as exc:
        print(exc)
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)
