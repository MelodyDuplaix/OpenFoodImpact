import time
import os
import sys
import logging
from typing import Callable
from fastapi import FastAPI, Depends, Request
from fastapi.responses import RedirectResponse
from routers import secure, public

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.auth import user_router, get_current_user

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(module)s %(message)s')


app = FastAPI(
    title="DataFoodImpact API",
    description="""API for managing recipes and products, with their nutritional and environmental information. 
    You can create, update, and retrieve recipes and products, as well as manage user accounts.""",
    version="1.0.0",
    openapi_tags=[
        {"name": "Public", "description": "Public routes"},
        {"name": "Updates", "description": "Secured routes only accessible to authenticated users, to update data"},
        {"name": "User", "description": "User management routes"}
    ]
)

@app.middleware("http")
async def add_timer_middleware(request: Request, call_next: Callable):
    """
    Middleware pour ajouter un en-tête X-Execution-Time à chaque réponse.

    Args:
        request (Request): Requête entrante.
        call_next (Callable): Prochain appel dans le pipeline de la requête.
    Returns:
        Response: Réponse avec l'en-tête X-Execution-Time ajouté.
    """
    start_time = time.time()
    response = await call_next(request)
    end_time = time.time()
    elapsed_time = end_time - start_time
    response.headers['X-Execution-Time'] = str(round(elapsed_time, 2))
    return response

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")

app.include_router(
    public.router,
    prefix="/api/public",
    tags=["Public"]
)
app.include_router(
    secure.router,
    prefix="/api/secure",
    dependencies=[Depends(get_current_user)],
)
app.include_router(user_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, workers=1, log_level="info")