from app.providers.kafka.builds.topic import (
    TopicBuilder,
)
from app.responses.response import (
    failed_response,
    success_response,
)


async def handle_create_topic(request):
    data = await request.post()

    topic_name = data.get("topic_name")
    bootstrap_servers = data.get("bootstrap_servers")

    num_partitions = int(data.get("num_partitions", 1))
    replication_factor = int(data.get("replication_factor", 1))
    timeout_ms = int(data.get("timeout_ms", 10000))

    log_data = {
        "topic_name": topic_name,
        "bootstrap_servers": bootstrap_servers,
        "num_partitions": num_partitions,
        "replication_factor": replication_factor,
        "timeout_ms": timeout_ms,
    }
    try:
        (
            TopicBuilder()
            .set_topic_name(topic_name)
            .set_bootstrap_servers(bootstrap_servers)
            .set_num_partitions(num_partitions)
            .set_replication_factor(replication_factor)
            .set_timeout_ms(timeout_ms)
            .build()
        )
    except Exception as exc:
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)

    log_data.update({"msg": "Topic is created"})
    return success_response(data=log_data)


async def handle_topic_info(request):
    data = await request.post()

    topic_name = data.get("topic_name")
    bootstrap_servers = data.get("bootstrap_servers")

    log_data = {
        "topic_name": topic_name,
        "bootstrap_servers": bootstrap_servers,
    }

    try:
        topic_info = (
            TopicBuilder()
            .set_topic_name(topic_name)
            .set_bootstrap_servers(bootstrap_servers)
            .info()
        )
        log_data["info"] = topic_info
        return success_response(data=log_data)

    except Exception as exc:
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)


async def handle_delete_topic(request):
    topic_name = request.query.get("topic_name")
    bootstrap_servers = request.query.get("bootstrap_servers")

    log_data = {
        "topic_name": topic_name,
        "bootstrap_servers": bootstrap_servers,
    }

    try:
        (
            TopicBuilder()
            .set_topic_name(topic_name)
            .set_bootstrap_servers(bootstrap_servers)
            .delete()
        )
    except Exception as exc:
        log_data.update({"msg": str(exc)})
        return failed_response(data=log_data)

    log_data.update({"msg": "Topic is deleted"})
    return success_response(data=log_data)
