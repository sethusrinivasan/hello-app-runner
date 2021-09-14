from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.config import Config
import uvicorn
from os import getenv, urandom, path, environ

async def homepage(request):
    return JSONResponse({'hello': 'world'})


app = Starlette(debug=True, routes=[
    Route('/', homepage),
])


config = Config()

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0",
                port=int(getenv('PORT', 8000)),
                log_level=getenv('LOG_LEVEL', "info"),
                debug=getenv('DEBUG', False),
                proxy_headers=True)
