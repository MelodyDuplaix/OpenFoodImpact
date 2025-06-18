from fastapi import Security, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.db import decode_access_token, get_user_by_username # get_user_by_username will be the new SQLAlchemy version
from api.services.db_session import get_db # Import the SQLAlchemy session dependency

bearer_scheme = HTTPBearer()

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
    user = get_user_by_username(db, username) # Pass the db session
    if user:
        # user is now an ORM object
        return {"id": user.id, "username": user.username, "user_level": user.user_level}
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials"
    )
