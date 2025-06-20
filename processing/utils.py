import os
from typing import Dict, Any
import psycopg2
import unicodedata
import logging
from sentence_transformers import SentenceTransformer
import re

def handle_error(e, context=None):
    """
    Centralise la gestion et la journalisation des erreurs.

    Args:
        e (Exception): The exception to handle.
        context (str, optional): Additional context for the error.
    Returns:
        None: Lève l'exception après l'avoir journalisée.
    """
    msg = f"Erreur: {e}"
    if context:
        msg += f" | Context: {context}"
    logging.error(msg)
    raise e # type: ignore

def get_db_connection():
    """
    Établit une connexion à la base de données PostgreSQL via les variables d'environnement.

    Args:
        None
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
        handle_error(e, 'Connexion DB') # type: ignore

STOPWORDS = {
    "de", "du", "des", "d'", "la", "le", "les", "l'", "en", "avec", "et", "à", "au", "aux", "un", "une", "-"
}

UNITES = [
    "g", "grammes", "kg", "mg", "ml", "cl", "l",
    "cuillère", "cuillères", "soupe", "café",
    "pincée", "pincées", "verre", "verres", "centimètre", "centimètres", "cm", "pincee"
    "tranche", "tranches", "boîte", "boîtes", "sachet", "sachets", "pot", "pots", "filet", "filets", "grosse", "grosses", "petite", "petites", "grande", "grandes",
    "cs", "cc", "tbsp", "tsp", "cup", "cups", "oz", "lb", "ounce", "pound", "liter", "litre", "liter",
    "piece", "pieces", "slice", "slices", "can", "cans", "bunch", "bunches", "clove", "cloves", "head", "heads"
]

UNIT_TO_GRAMS_APPROX = {
    "g": 1, "gramme": 1, "grammes": 1,
    "kg": 1000, "kilogramme": 1000, "kilogrammes": 1000,
    "mg": 0.001,
    "ml": 1,
    "cl": 10,
    "l": 1000, "litre": 1000, "litres": 1000,
    "cuillère à soupe": 15, "cuillères à soupe": 15, "cs": 15, "tbsp": 15,
    "cuillère à café": 5, "cuillères à café": 5, "cc": 5, "tsp": 5,
    "pincée": 0.5, "pincées": 0.5, "pincee": 0.5,
    "verre": 150, "verres": 150, "cup": 240, "cups": 240,
    "pot": 125,
    "sachet": 10,
    "gousse": 5,
    "tranche": 20, "tranches": 20, "slice": 20, "slices": 20,
    "oz": 28.35, "ounce": 28.35,
    "lb": 453.59, "pound": 453.59,
    "filet": 10,
    "morceau": 50, "morceaux": 50, "piece": 50, "pieces": 50,
    "boîte": 200, "boîtes": 200, "can": 400, "cans": 400,
    "botte": 100, "bunch": 100, "bunches": 100,
    "tête": 100, "head": 100, "heads": 100,
    "un": 1, "une": 1, "deux": 2, "trois": 3, "quatre": 4, "cinq": 5, "six": 6, "sept": 7, "huit": 8, "neuf": 9, "dix": 10,
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}

DEFAULT_QUANTITY_GRAMS = 100

def normalize_name(texte):
    """Normalize a product name (lowercase, remove accents, special chars).

    Args:
        texte (str): Chaîne de caractères à normaliser.
    Returns:
        str: Chaîne normalisée (minuscules, sans accents, sans caractères spéciaux, etc.).
    Utilisée pour créer une clé de matching pour product_vector, nettoyage agressif.
    """
    if not texte:
        return ""

    texte = texte.lower()
    if not isinstance(texte, str): texte = ""

    # on enlève les parenthèses et le contenu entre parenthèses
    texte = re.sub(r"\s*\([^)]*\)", "", texte)
    if not isinstance(texte, str): texte = ""
    texte = texte.strip()

    # on enlève les traits d'union et les slashs
    split_result = re.split(r"[-/]|\s+ou\s+", texte)
    if split_result:
        texte = split_result[0]
        if not isinstance(texte, str):
             texte = ""
    else:
        texte = ""
    texte = texte.strip()

    # on enlève les unités de mesure
    pattern_unit = r"\b\d+([.,]\d+)?\s*(" + "|".join(UNITES) + r")\b"
    texte = re.sub(pattern_unit, "", texte)
    if not isinstance(texte, str): texte = ""

    # on enlève les nombres
    texte = re.sub(r"\d+([.,]\d+)?", "", texte)
    if not isinstance(texte, str): texte = ""

    # on enlève les accents
    texte = re.sub(r"[^a-zàâäéèêëïîôöùûüç\s-]", "", texte)
    if not isinstance(texte, str): texte = ""

    ADJECTIFS = {"frais", "fraiche", "fraîche", "bio", "entier", "entiere", 
                 "petit", "petite", "grand", "grande", "moyen", "moyenne", "sec", "sèche", "moelleux", "moelleuse", "demi", "demie", "nouveau", 
                 "nouvelle", "vieux", "vieille", "jeune", "rond", "ronde", "long", "longue", "court", "courte", "gros", "grosse", "fin", "fine", 
                 "épais", "épaisse", "blanc", "blanche", "rouge", "jaune", "vert", "verte", "noir", "noire", "rose", "violet", "violette", "orange", 
                 "doré", "dorée", "brun", "brune", "cru", "crue", "cuit", "cuite", "surgelé", "surgelée", "nature", "complet", "complète", "allégé", 
                 "allégée", "léger", "légère", "extra", "double", "triple", "simple", "sec", "secs", "sèche", "sèches"}
    QUANTITES = {"quelques", "beaucoup", "peu", "plusieurs", "moitié", "quart", "tiers", "demi", "entier", "entière"}
    mots = texte.split()
    # on enlève les stopwords, les adjectifs et les quantités
    mots_nettoyes = [mot for mot in mots if mot not in STOPWORDS and mot not in ADJECTIFS and mot not in QUANTITES]

    mots_nettoyes = [mot for mot in mots_nettoyes if isinstance(mot, str)]
    # on ne garde que les caractères ascii et on enlève les accents
    mots_nettoyes = [unicodedata.normalize('NFD', mot).encode('ascii', 'ignore').decode('utf-8') for mot in mots_nettoyes]
    final_string = " ".join(mots_nettoyes)
    return final_string.strip()

def parse_ingredient_details_fr_en(ingredient_string: str) -> Dict[str, Any]:
    """
    Extrait quantité, unité, nom et quantité en grammes d'une chaîne d'ingrédient.

    Args:
        ingredient_string (str): Chaîne décrivant l'ingrédient (ex: "250g de sucre").
    Returns:
        Dict[str, Any]: Dictionnaire avec "raw_text", "quantity_str", "unit_str",
                        "parsed_name", et "quantity_grams".
    La conversion en grammes est approximative.
    """
    original_string = ingredient_string
    text = ingredient_string.lower()

    quantity_str = None
    unit_str = None
    quantity_grams = None
    ingredient_name_part = text

    explicit_units_pattern = r"\b(" + "|".join(re.escape(u) for u in [
        "g", "gramme", "grammes", "kg", "kilogramme", "kilogrammes", "mg", 
        "ml", "cl", "l", "litre", "litres", "cuillère", "cuillères", "cs", "cc", "tbsp", "tsp",
        "pincée", "pincées", "pincee", "verre", "verres", "cup", "cups", "oz", "lb", "ounce", "pound",
        "sachet", "sachets", "boîte", "boîtes", "can", "cans", "pot", "pots", "gousse", "gousses",
        "tête", "têtes", "head", "heads", "filet", "filets", "tranche", "tranches", "slice", "slices",
        "morceau", "morceaux", "piece", "pieces", "botte", "bottes", "bunch", "bunches"
    ]) + r")\b"

    regex_qty_unit = rf"^((\d+[\.,]\d*|\d+/\d+|\d+)\s*({explicit_units_pattern})?\b)\s*(.*)"
    regex_text_unit = r"^(une?|deux|trois|quatre|cinq|six|sept|huit|neuf|dix)\s+([a-zA-Zàâäéèêëïîôöùûüç\s\.\-\']+?)\s+(de|d')\s+(.*)"

    # on vérifie si le texte correspond à un format de quantité explicite
    match_text_unit = re.match(regex_text_unit, text)
    if match_text_unit:
        # si c'est le cas, on extrait la quantité et l'unité
        quantity_str = match_text_unit.group(1).strip()
        unit_candidate = match_text_unit.group(2).strip()
        if re.fullmatch(explicit_units_pattern, unit_candidate, re.IGNORECASE):
            unit_str = unit_candidate
            ingredient_name_part = match_text_unit.group(4).strip()
        else:
            unit_str = None
            ingredient_name_part = text
    else:
        # sinon, on utilise le regex pour quantité et unité, on vérifie si on a une unité explicite
        match_qty_unit = re.match(regex_qty_unit, text)
        if match_qty_unit:
            # si c'est le cas, on extrait la quantité et l'unité
            quantity_match_in_group2 = re.match(r"(\d+[\.,]\d*|\d+/\d+|\d+)", match_qty_unit.group(2).strip())
            if quantity_match_in_group2:
                quantity_str = quantity_match_in_group2.group(1)
            
            unit_str_candidate_from_qty_regex = match_qty_unit.group(3)
            if unit_str_candidate_from_qty_regex:
                unit_str = unit_str_candidate_from_qty_regex.strip()
            
            name_group_match = match_qty_unit.group(4)
            if name_group_match is not None:
                ingredient_name_part = name_group_match.strip()
            else:
                ingredient_name_part = ""
        else:
            # si aucune quantité explicite n'est trouvée, on laisse l'unité et la quantité à None
            ingredient_name_part = text.strip()

    # on enlève les morts de liaisons
    if ingredient_name_part.startswith("de "):
        ingredient_name_part = ingredient_name_part[3:]
    elif ingredient_name_part.startswith("d'"):
        ingredient_name_part = ingredient_name_part[2:]
    
    # on enlève les parenthèses et le contenu entre parenthèses
    ingredient_name_part = re.sub(r"\s*\([^)]*\)", "", ingredient_name_part).strip()
    words = ingredient_name_part.split()
    cleaned_name = " ".join(words).strip()

    # si il y a des quantités et unités qu'on connait, on les convertit en grammes
    if quantity_str and unit_str and unit_str in UNIT_TO_GRAMS_APPROX:
        try:
            if "/" in quantity_str:
                num, den = map(float, quantity_str.split('/'))
                q_val = num / den
            else:
                q_val = float(quantity_str.replace(",", "."))
            quantity_grams = q_val * UNIT_TO_GRAMS_APPROX[unit_str]
        except ValueError:
            pass
    elif quantity_str and not unit_str:
        # si on a une quantité mais pas d'unité, on considère que c'est en grammes
        try:
            q_val = float(quantity_str.replace(",", "."))
            quantity_grams = q_val
        except ValueError:
            pass

    return {
        "raw_text": original_string,
        "quantity_str": quantity_str,
        "unit_str": unit_str,
        "parsed_name": cleaned_name,
        "quantity_grams": quantity_grams if quantity_grams is not None else (DEFAULT_QUANTITY_GRAMS if quantity_str else None)
    }

def vectorize_name(name):
    """
    Vectorise un nom de produit en utilisant un modèle SentenceTransformer.

    Args:
        name (str): The name to vectorize.
    Returns:
        list: Vector representation
    """
    if not hasattr(vectorize_name, 'model'):
        vectorize_name.model = SentenceTransformer('all-MiniLM-L6-v2') # type: ignore
    return vectorize_name.model.encode([name], show_progress_bar=False)[0].tolist() # type: ignore

def safe_execute(cur, sql, params=None):
    """
    Exécute une requête SQL avec gestion d'erreur centralisée.

    Args:
        cur (psycopg2.cursor): Database cursor
        sql (str): SQL query
        params (tuple, optional): Query parameters
    Returns:
        None: Exécute la requête ou lève une exception via handle_error.
    """
    try:
        cur.execute(sql, params)
    except Exception as e:
        handle_error(e, f'SQL: {sql} | Params: {params}')
