import os
import psycopg2
import unicodedata
import logging
from sentence_transformers import SentenceTransformer

def get_db_connection():
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
        logging.error(f'Erreur connexion DB: {e}')
        raise

def normalize_name(name):
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
    if not hasattr(vectorize_name, 'model'):
        vectorize_name.model = SentenceTransformer('all-MiniLM-L6-v2')
    return vectorize_name.model.encode([name], show_progress_bar=False)[0].tolist()

def safe_execute(cur, sql, params=None):
    try:
        cur.execute(sql, params)
    except Exception as e:
        logging.error(f'Erreur SQL: {e}\nRequête: {sql}\nParams: {params}')
        raise
