from aiohttp import web
from app.background import shared_generator
from app.responses.response import success_response


async def handle_stats(request):
    """GET /stats — return cumulative generation stats for this session."""
    return success_response(data=shared_generator.stats.to_dict())


async def handle_stats_reset(request):
    """POST /stats/reset — reset cumulative stats back to zero."""
    shared_generator.stats.reset()
    return success_response(data={"msg": "Stats reset successfully."})


def setup_routes(app):
    app.router.add_get("/stats",        handle_stats)
    app.router.add_post("/stats/reset", handle_stats_reset)
