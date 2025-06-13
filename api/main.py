import re
from fastapi import FastAPI, Depends, Request
from routers import secure, public
from auth import get_user
from fastapi import status
from fastapi.responses import JSONResponse
from fastapi import APIRouter
import os
import sys
from pydantic import BaseModel

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.db import get_user_by_username, create_user, verify_password, create_access_token

user_router = APIRouter(prefix="/api/user", tags=["User"])

class UserAuthRequest(BaseModel):
    username: str
    password: str

@user_router.post("/register", response_model=dict)
async def register(body: UserAuthRequest):
    username = body.username
    password = body.password
    if not username or not password:
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": "Username and password required"})
    user = get_user_by_username(username)
    if user:
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"detail": "Username already exists"})
    result = create_user(username, password)
    if result:
        token = create_access_token({"sub": username, "user_id": result["user_id"]})
        return {"user_id": result["user_id"], "username": username, "access_token": token, "token_type": "bearer", "message": "Registration successful"}
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": "Registration failed"})

@user_router.post("/login", response_model=dict)
async def login(body: UserAuthRequest):
    username = body.username
    password = body.password
    if not username or not password:
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": "Username and password required"})
    user = get_user_by_username(username)
    if user and verify_password(password, user[2]):
        token = create_access_token({"sub": username, "user_id": user[0], "user_level": user[4]})
        return {"user_id": user[0], "username": user[1], "access_token": token, "token_type": "bearer", "user_level": user[4], "message": "Login successful"}
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

app.include_router(
    public.router,
    prefix="/api/public",
    tags=["Public"]
)
app.include_router(
    secure.router,
    prefix="/api/secure",
    dependencies=[Depends(get_user)],
    tags=["Secure"]
)
app.include_router(user_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, workers=1, log_level="info")