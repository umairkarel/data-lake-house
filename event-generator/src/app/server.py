from aiohttp import web

from app.providers import (
    setup_kafka_providers_routes,
)
from app.routes import (
    setup_routes,
)


from app.background import start_background_tasks, cleanup_background_tasks

def create_app():
    app = web.Application()
    setup_routes(app)
    setup_kafka_providers_routes(app)
    
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    
    return app
