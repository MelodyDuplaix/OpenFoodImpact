import os
import psycopg2
import unicodedata
import logging
from sentence_transformers import SentenceTransformer
import re

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

STOPWORDS = {
    "de", "du", "des", "d'", "la", "le", "les", "l'", "en", "avec", "et", "à", "au", "aux", "un", "une", "-"
}

UNITES = [
    "g", "grammes", "kg", "mg", "ml", "cl", "l",
    "cuillère", "cuillères", "soupe", "café",
    "pincée", "pincées", "verre", "verres", "centimètre", "centimètres", "cm", "pincee"
    "tranche", "tranches", "boîte", "boîtes", "sachet", "sachets", "pot", "pots", "filet", "filets", "grosse", "grosses", "petite", "petites", "grande", "grandes",
]

def normalize_name(texte):
    """Normalize a product name (lowercase, remove accents, special chars).

    Args:
        name (str): The name to normalize.

    Returns:
        str: Normalized name
    """
    # Mise en minuscule
    texte = texte.lower()
    # Supprimer le contenu entre parenthèses
    texte = re.sub(r"\([^)]*\)", "", texte)
    # Remplacer les tirets, slashs, ' ou ' par un espace (on ne garde que le premier ingrédient)
    texte = re.split(r"[-/]|\bou\b", texte)[0]
    # Supprimer les quantités et unités
    pattern_unit = r"\b\d+([.,]\d+)?\s*(" + "|".join(UNITES) + r")\b"
    texte = re.sub(pattern_unit, "", texte)
    # Supprimer les chiffres
    texte = re.sub(r"\d+([.,]\d+)?", "", texte)
    # Supprimer les caractères spéciaux
    texte = re.sub(r"[^a-zàâäéèêëïîôöùûüç\s-]", "", texte)
    # Supprimer les adjectifs et mots inutiles
    ADJECTIFS = {"frais", "fraiche", "fraîche", "bio", "entier", "entiere", "petit", "petite", "grand", "grande", "moyen", "moyenne", "sec", "sèche", "moelleux", "moelleuse", "demi", "demie", "nouveau", "nouvelle", "vieux", "vieille", "jeune", "rond", "ronde", "long", "longue", "court", "courte", "gros", "grosse", "fin", "fine", "épais", "épaisse", "blanc", "blanche", "rouge", "jaune", "vert", "verte", "noir", "noire", "rose", "violet", "violette", "orange", "doré", "dorée", "brun", "brune", "cru", "crue", "cuit", "cuite", "surgelé", "surgelée", "nature", "complet", "complète", "allégé", "allégée", "léger", "légère", "extra", "double", "triple", "simple", "sec", "secs", "sèche", "sèches"}
    QUANTITES = {"quelques", "beaucoup", "peu", "plusieurs", "moitié", "quart", "tiers", "demi", "entier", "entière"}
    # Supprimer les stopwords, adjectifs, quantités, préparation
    mots = texte.split()
    mots_nettoyes = [mot for mot in mots if mot not in STOPWORDS and mot not in ADJECTIFS and mot not in QUANTITES]
    # Supprimer les accents
    mots_nettoyes = [unicodedata.normalize('NFD', mot).encode('ascii', 'ignore').decode('utf-8') for mot in mots_nettoyes]
    # Nettoyer les espaces multiples et bords
    return " ".join(mots_nettoyes).strip()

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
