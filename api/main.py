import re
import time
from typing import Callable
from fastapi import FastAPI, Depends, Request, HTTPException
from routers import secure, public
from auth import get_current_user # Renamed for clarity
from fastapi import status
from fastapi.responses import JSONResponse
from fastapi import APIRouter
import os
import sys
from pydantic import BaseModel
from sqlalchemy.orm import Session

import logging
logger = logging.getLogger(__name__)
level = getattr(logging, "INFO", None)
logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(module)s %(message)s')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.db import get_user_by_username, create_user, verify_password, create_access_token
from api.services.db_session import get_db, init_db # Import get_db and init_db

user_router = APIRouter(prefix="/api/user", tags=["User"])


class UserAuthRequest(BaseModel):
    username: str
    password: str

@user_router.post("/register", response_model=dict)
async def register(body: UserAuthRequest, db: Session = Depends(get_db)):
    """
    Enregistre un nouvel utilisateur.

    Args:
        body (UserAuthRequest): Données d'enregistrement (username, password).
    Returns:
        dict: Informations sur l'utilisateur et token d'accès en cas de succès.
    Raises:
        JSONResponse: En cas d'erreur (champs manquants, utilisateur existant, etc.).
    """
    username = body.username
    password = body.password
    if not username or not password:
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": "Username and password required"})
    db_user = get_user_by_username(db, username)
    if db_user:
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"detail": "Username already exists"})
    new_user = create_user(db, username, password)
    if new_user:
        token = create_access_token({"sub": new_user.username, "user_id": new_user.id, "user_level": new_user.user_level})
        return {"user_id": new_user.id, "username": new_user.username, "access_token": token, "token_type": "bearer", "user_level": new_user.user_level, "message": "Registration successful"}
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": "Registration failed"})

@user_router.post("/login", response_model=dict)
async def login(body: UserAuthRequest, db: Session = Depends(get_db)):
    """
    Connecte un utilisateur existant.

    Args:
        body (UserAuthRequest): Données de connexion (username, password).
    Returns:
        dict: Informations sur l'utilisateur et token d'accès en cas de succès.
    Raises:
        JSONResponse: En cas d'échec de l'authentification.
    """
    username = body.username
    password = body.password
    if not username or not password:
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": "Username and password required"})
    user = get_user_by_username(db, username)
    if user and verify_password(password, user.password): # user.password is the hashed password
        token = create_access_token({"sub": user.username, "user_id": user.id, "user_level": user.user_level})
        return {"user_id": user.id, "username": user.username, "access_token": token, "token_type": "bearer", "user_level": user.user_level, "message": "Login successful"}
    return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Invalid credentials"})

app = FastAPI(
    title="DataFoodImpact API",
    description="API for managing recipes and products, with their nutritional and environmental information.",
    version="1.0.0",
    openapi_tags=[
        {"name": "Public", "description": "Public routes"},
        {"name": "Secure", "description": "Secured routes only accessible to authenticated users"},
        {"name": "User", "description": "User management routes"}
    ]
)

@app.on_event("startup")
async def on_startup():
    # init_db() # Décommentez pour créer les tables au démarrage si elles n'existent pas.
    pass # Vous pouvez appeler init_db() ici si nécessaire, mais c'est souvent fait hors ligne.

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

app.include_router(
    public.router,
    prefix="/api/public",
    tags=["Public"]
)
app.include_router(
    secure.router,
    prefix="/api/secure",
    dependencies=[Depends(get_current_user)], # Use the renamed dependency
    tags=["Secure"]
)
app.include_router(user_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, workers=1, log_level="info")