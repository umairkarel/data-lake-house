"""
REST handler for POST /kafka/generateMessages

Triggers a one-shot batch of order_events via the REST API.
Uses the shared OrderEventGenerator so stats accumulate across all calls.
"""
from time import time

from app.background import shared_generator
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
        s = time()

        # Snapshot stats before generation so we can diff what this batch produced
        before_total = shared_generator.stats.total_events
        before_late  = shared_generator.stats.late_events

        events = shared_generator.generate_batch(count)

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

        batch_total = shared_generator.stats.total_events - before_total
        batch_late  = shared_generator.stats.late_events  - before_late

        log_data.update({
            "msg":         "Messages published successfully",
            "elapsed_sec": round(time() - s, 3),
            "batch_summary": {
                "generated":    batch_total,
                "normal":       batch_total - batch_late,
                "late":         batch_late,
                "late_pct":     f"{round(batch_late / batch_total * 100, 1) if batch_total else 0}%",
            },
            "session_stats": shared_generator.stats.to_dict(),
        })
        return success_response(data=log_data)

    except Exception as exc:
        print(exc)
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)
