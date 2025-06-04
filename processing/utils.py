import os
import psycopg2
import unicodedata
import logging
from sentence_transformers import SentenceTransformer

def handle_error(e, context=None):
    """Centralise error handling and logging.

    Args:
        e (Exception): The exception to handle.
        context (str, optional): Additional context for the error.

    Returns:
        None
    """
    msg = f"Erreur: {e}"
    if context:
        msg += f" | Context: {context}"
    logging.error(msg)
    raise e

def get_db_connection():
    """Get a PostgreSQL database connection using environment variables.

    Returns:
        psycopg2.extensions.connection: Database connection object
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB', 'postgres'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        return conn
    except Exception as e:
        handle_error(e, 'Connexion DB')

def normalize_name(name):
    """Normalize a product name (lowercase, remove accents, special chars).

    Args:
        name (str): The name to normalize.

    Returns:
        str: Normalized name
    """
    if not isinstance(name, str):
        return ''
    name = name.lower()
    name = unicodedata.normalize('NFKD', name)
    name = ''.join([c for c in name if not unicodedata.combining(c)])
    import re
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    name = ' '.join(name.split())
    return name

def vectorize_name(name):
    """Vectorize a product name using a sentence transformer.

    Args:
        name (str): The name to vectorize.

    Returns:
        list: Vector representation
    """
    if not hasattr(vectorize_name, 'model'):
        vectorize_name.model = SentenceTransformer('all-MiniLM-L6-v2')
    return vectorize_name.model.encode([name], show_progress_bar=False)[0].tolist()

def safe_execute(cur, sql, params=None):
    """Execute a SQL statement safely with error handling.

    Args:
        cur (psycopg2.cursor): Database cursor
        sql (str): SQL query
        params (tuple, optional): Query parameters

    Returns:
        None
    """
    try:
        cur.execute(sql, params)
    except Exception as e:
        handle_error(e, f'SQL: {sql} | Params: {params}')
