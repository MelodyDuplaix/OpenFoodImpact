from fastapi import Security, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.db import decode_access_token, get_user_by_username

bearer_scheme = HTTPBearer()

def get_user(credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)):
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
    user = get_user_by_username(username)
    if user:
        # user = (id, username, password, user_level)
        return {"id": user[0], "username": user[1], "user_level": user[3]}
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials"
    )
