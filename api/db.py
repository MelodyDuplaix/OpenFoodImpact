import psycopg2
import os
from processing.utils import get_db_connection
import secrets
from passlib.context import CryptContext
from datetime import datetime, timedelta
from jose import jwt
from fastapi import HTTPException
from typing import Optional
from pymongo import MongoClient

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24

def get_user_by_username(username: str):
    """
    Récupère un utilisateur par son nom d'utilisateur depuis la base de données.

    Args:
        username (str): Nom d'utilisateur à rechercher.
    Returns:
        tuple or None: Tuple contenant (id, username, password_hash, user_level)
                       ou None si l'utilisateur n'est pas trouvé ou en cas d'erreur.
    """
    conn = get_db_connection()
    if not conn:
        return None
    cur = conn.cursor()
    cur.execute("SELECT id, username, password, user_level FROM users WHERE username = %s", (username,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user

def create_user(username: str, password: str, user_level: str = "user"):
    """
    Crée un nouvel utilisateur dans la base de données.

    Args:
        username (str): Nom d'utilisateur.
        password (str): Mot de passe en clair (sera haché).
        user_level (str, optional): Niveau de l'utilisateur. Défaut à "user".
    Returns:
        dict or None: Dictionnaire {"user_id": id} en cas de succès, sinon None.
    """
    conn = get_db_connection()
    if not conn:
        return None
    cur = conn.cursor()
    hashed_password = pwd_context.hash(password)
    user_id = None
    try:
        cur.execute("INSERT INTO users (username, password, user_level) VALUES (%s, %s, %s) RETURNING id", (username, hashed_password, user_level))
        result = cur.fetchone()
        if result:
            user_id = result[0]
        conn.commit()
    except Exception as e:
        conn.rollback()
        user_id = None
    cur.close()
    conn.close()
    if user_id:
        return {"user_id": user_id}
    return None

def verify_password(plain_password, hashed_password):
    """
    Vérifie un mot de passe en clair contre un mot de passe haché.

    Args:
        plain_password (str): Mot de passe en clair.
        hashed_password (str): Mot de passe haché.
    Returns:
        bool: True si les mots de passe correspondent, False sinon.
    """
    return pwd_context.verify(plain_password, hashed_password)
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """
    Crée un token d'accès JWT.

    Args:
        data (dict): Données à encoder dans le token (typiquement le 'sub').
        expires_delta (Optional[timedelta], optional): Durée de validité du token.
                                                      Défaut à ACCESS_TOKEN_EXPIRE_MINUTES.
    Returns:
        str: Token JWT encodé.
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(token: str):
    """
    Décode un token d'accès JWT.

    Args:
        token (str): Token JWT à décoder.
    Returns:
        dict: Payload du token décodé.
    Raises:
        HTTPException: Si le token est invalide (expiré, malformé, etc.).
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


def get_mongodb_connection():
    """
    Établit une connexion client à MongoDB via une variable d'environnement.

    Args:
        None
    Returns:
        pymongo.MongoClient or None: Client MongoDB en cas de succès, sinon None.
    """
    try:
        client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None