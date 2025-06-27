from fastapi import Security, HTTPException, status, Depends, APIRouter
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel
import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.db import get_user_by_username, create_user, verify_password, create_access_token, decode_access_token
from api.services.db_session import get_db

bearer_scheme = HTTPBearer()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(module)s %(message)s')

def get_current_user(db: Session = Security(get_db), credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)):
    """
    Valide le token JWT et retourne les informations de l'utilisateur.

    Args:
        credentials (HTTPAuthorizationCredentials): Identifiants d'autorisation Bearer.
    Returns:
        dict: Dictionnaire contenant id, username, et user_level de l'utilisateur.
    Raises:
        HTTPException: Si le token est invalide ou l'utilisateur n'est pas trouvé.
    """
    token = credentials.credentials
    payload = decode_access_token(token)
    username = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials"
        )
    user = get_user_by_username(db, username)
    if user:
        return {"id": user.id, "username": user.username, "user_level": user.user_level}
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials"
    )



user_router = APIRouter(prefix="/api/user", tags=["User"])


class UserAuthRequest(BaseModel):
    username: str
    password: str

@user_router.post("/register", response_model=dict)
async def register(body: UserAuthRequest, db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)):
    """
    Register a new user.  

    Args:  
        body (UserAuthRequest): Registration data (username, password).  
        current_user (dict): Authenticated user info (must be admin).
    Returns:  
        dict: User info and access token if successful.  
    Raises:  
        JSONResponse: On error (missing fields, user exists, etc.).  
    """
    if current_user.get("user_level") != "admin":
        return JSONResponse(status_code=status.HTTP_403_FORBIDDEN, content={"detail": "Only admin users can create new accounts."})
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
    Log in an existing user.  

    Args:  
        body (UserAuthRequest): Login data (username, password).  
    Returns:  
        dict: User info and access token if successful.  
    Raises:  
        JSONResponse: On authentication failure.  
    """
    username = body.username
    password = body.password
    if not username or not password:
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"detail": "Username and password required"})
    user = get_user_by_username(db, username)
    if user and verify_password(password, user.password):
        token = create_access_token({"sub": user.username, "user_id": user.id, "user_level": user.user_level})
        return {"user_id": user.id, "username": user.username, "access_token": token, "token_type": "bearer", "user_level": user.user_level, "message": "Login successful"}
    return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Invalid credentials"})

@user_router.delete("/delete_account", response_model=dict)
async def delete_account(db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)):
    """
    Delete the currently authenticated user's account.

    Args:
        current_user (dict): Authenticated user info.
    Returns:
        dict: Confirmation message.
    Raises:
        JSONResponse: On error (user not found or deletion failed).
    """
    user = get_user_by_username(db, current_user["username"])
    if not user:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": "User not found"})
    try:
        db.delete(user)
        db.commit()
        return {"message": "Account deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": "Account deletion failed"})
