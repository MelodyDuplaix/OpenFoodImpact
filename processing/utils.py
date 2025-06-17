import os
from typing import Dict, Any
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
    "cs", "cc", "tbsp", "tsp", "cup", "cups", "oz", "lb", "ounce", "pound", "liter", "litre", "liter",
    "piece", "pieces", "slice", "slices", "can", "cans", "bunch", "bunches", "clove", "cloves", "head", "heads"
]

# Table de conversion approximative en grammes (très heuristique)
UNIT_TO_GRAMS_APPROX = {
    "g": 1, "gramme": 1, "grammes": 1,
    "kg": 1000, "kilogramme": 1000, "kilogrammes": 1000,
    "mg": 0.001,
    "ml": 1,  # En supposant densité de l'eau pour liquides
    "cl": 10,
    "l": 1000, "litre": 1000, "litres": 1000,
    "cuillère à soupe": 15, "cuillères à soupe": 15, "cs": 15, "tbsp": 15,
    "cuillère à café": 5, "cuillères à café": 5, "cc": 5, "tsp": 5,
    "pincée": 0.5, "pincées": 0.5, "pincee": 0.5,
    "verre": 150, "verres": 150, "cup": 240, "cups": 240, # Très variable
    "pot": 125, # Ex: pot de yaourt
    "sachet": 10, # Ex: sachet de levure
    "gousse": 5, # Ex: gousse d'ail
    "tranche": 20, "tranches": 20, "slice": 20, "slices": 20,
    "oz": 28.35, "ounce": 28.35,
    "lb": 453.59, "pound": 453.59,
    "filet": 10, # Ex: filet d'huile
    "morceau": 50, "morceaux": 50, "piece": 50, "pieces": 50,
    "boîte": 200, "boîtes": 200, "can": 400, "cans": 400,
    "botte": 100, "bunch": 100, "bunches": 100,
    "tête": 100, "head": 100, "heads": 100, # Ex: tête d'ail
    # Unités numériques directes
    "un": 1, "une": 1, "deux": 2, "trois": 3, "quatre": 4, "cinq": 5, "six": 6, "sept": 7, "huit": 8, "neuf": 9, "dix": 10,
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}

DEFAULT_QUANTITY_GRAMS = 100 # Si aucune quantité/unité n'est trouvée, on assume 100g pour les calculs

def normalize_name(texte):
    """Normalize a product name (lowercase, remove accents, special chars).
    Cette fonction est utilisée pour créer une clé de matching pour product_vector.
    Elle est agressive dans le nettoyage.

    Args:
        name (str): The name to normalize.

    Returns:
        str: Normalized name
    """
    if not texte: # Handle None or empty string input
        return ""

    # Mise en minuscule
    texte = texte.lower()
    if not isinstance(texte, str): texte = "" # Au cas où .lower() retournerait autre chose (improbable)

    # Supprimer le contenu entre parenthèses
    texte = re.sub(r"\s*\([^)]*\)", "", texte)
    if not isinstance(texte, str): texte = ""
    texte = texte.strip()

    # Remplacer les tirets, slashs, ' ou ' par un espace (on ne garde que le premier ingrédient)
    split_result = re.split(r"[-/]|\s+ou\s+", texte)
    if split_result: # Ensure the list is not empty
        texte = split_result[0]
        if not isinstance(texte, str): # Ensure the first element is a string
             texte = "" # Or handle as an error, but empty string is safer for subsequent steps
    else:
        texte = "" # If split resulted in an empty list, the result is an empty string
    texte = texte.strip() # Assurer que texte est une chaîne avant .strip()

    # Supprimer les quantités et unités (déjà fait dans parse_ingredient_details_fr_en si utilisé avant)
    pattern_unit = r"\b\d+([.,]\d+)?\s*(" + "|".join(UNITES) + r")\b"
    texte = re.sub(pattern_unit, "", texte)
    if not isinstance(texte, str): texte = ""

    # Supprimer les chiffres
    texte = re.sub(r"\d+([.,]\d+)?", "", texte)
    if not isinstance(texte, str): texte = ""

    # Supprimer les caractères spéciaux
    texte = re.sub(r"[^a-zàâäéèêëïîôöùûüç\s-]", "", texte)
    if not isinstance(texte, str): texte = ""

    # Supprimer les adjectifs et mots inutiles
    ADJECTIFS = {"frais", "fraiche", "fraîche", "bio", "entier", "entiere", "petit", "petite", "grand", "grande", "moyen", "moyenne", "sec", "sèche", "moelleux", "moelleuse", "demi", "demie", "nouveau", "nouvelle", "vieux", "vieille", "jeune", "rond", "ronde", "long", "longue", "court", "courte", "gros", "grosse", "fin", "fine", "épais", "épaisse", "blanc", "blanche", "rouge", "jaune", "vert", "verte", "noir", "noire", "rose", "violet", "violette", "orange", "doré", "dorée", "brun", "brune", "cru", "crue", "cuit", "cuite", "surgelé", "surgelée", "nature", "complet", "complète", "allégé", "allégée", "léger", "légère", "extra", "double", "triple", "simple", "sec", "secs", "sèche", "sèches"}
    QUANTITES = {"quelques", "beaucoup", "peu", "plusieurs", "moitié", "quart", "tiers", "demi", "entier", "entière"}
    # Supprimer les stopwords, adjectifs, quantités, préparation
    mots = texte.split()
    mots_nettoyes = [mot for mot in mots if mot not in STOPWORDS and mot not in ADJECTIFS and mot not in QUANTITES]

    # Ensure mots_nettoyes contains only strings before joining
    mots_nettoyes = [mot for mot in mots_nettoyes if isinstance(mot, str)]
    # Supprimer les accents
    # Ensure each mot is a string before normalization
    mots_nettoyes = [unicodedata.normalize('NFD', mot).encode('ascii', 'ignore').decode('utf-8') for mot in mots_nettoyes]
    # Nettoyer les espaces multiples et bords
    final_string = " ".join(mots_nettoyes)
    return final_string.strip()

def parse_ingredient_details_fr_en(ingredient_string: str) -> Dict[str, Any]:
    """
    Tente d'extraire la quantité, l'unité et le nom d'un ingrédient à partir d'une chaîne.
    Retourne également une quantité normalisée en grammes si possible.
    """
    original_string = ingredient_string
    text = ingredient_string.lower()

    quantity_str = None
    unit_str = None
    quantity_grams = None
    ingredient_name_part = text # Initialiser avec le texte complet

    # Liste explicite des mots qui peuvent être des unités (en minuscule)
    # Cela aide à éviter de capturer des noms d'ingrédients comme "citrons" ou "pommes" en tant qu'unités.
    explicit_units_pattern = r"\b(" + "|".join(re.escape(u) for u in [
        "g", "gramme", "grammes", "kg", "kilogramme", "kilogrammes", "mg", 
        "ml", "cl", "l", "litre", "litres", "cuillère", "cuillères", "cs", "cc", "tbsp", "tsp",
        "pincée", "pincées", "pincee", "verre", "verres", "cup", "cups", "oz", "lb", "ounce", "pound",
        "sachet", "sachets", "boîte", "boîtes", "can", "cans", "pot", "pots", "gousse", "gousses",
        "tête", "têtes", "head", "heads", "filet", "filets", "tranche", "tranches", "slice", "slices",
        "morceau", "morceaux", "piece", "pieces", "botte", "bottes", "bunch", "bunches"
    ]) + r")\b"

    # Regex pour quantité et unité (assez permissive)
    # Ex: "1 kg", "2.5", "1/2 litre", "2 cuillères à soupe"
    # Gère les nombres, les fractions simples, et les unités optionnelles
    # L'unité est maintenant optionnelle et doit correspondre à explicit_units_pattern si présente numériquement
    regex_qty_unit = rf"^((\d+[\.,]\d*|\d+/\d+|\d+)\s*({explicit_units_pattern})?\b)\s*(.*)"
    # Regex pour les unités textuelles comme "une pincée de"
    regex_text_unit = r"^(une?|deux|trois|quatre|cinq|six|sept|huit|neuf|dix)\s+([a-zA-Zàâäéèêëïîôöùûüç\s\.\-\']+?)\s+(de|d')\s+(.*)"

    match_text_unit = re.match(regex_text_unit, text)
    if match_text_unit:
        quantity_str = match_text_unit.group(1).strip() # "une", "deux", etc.
        unit_candidate = match_text_unit.group(2).strip()     # "pincée", "cuillère"
        # Vérifier si l'unité textuelle est une unité valide pour éviter de capturer des noms
        if re.fullmatch(explicit_units_pattern, unit_candidate, re.IGNORECASE):
            unit_str = unit_candidate
            ingredient_name_part = match_text_unit.group(4).strip()
        else:
            # Si ce n'est pas une unité explicite, on considère que c'est le début du nom de l'ingrédient
            unit_str = None # Réinitialiser unit_str
            ingredient_name_part = text # Reprendre le texte original pour le nom
    else:
        match_qty_unit = re.match(regex_qty_unit, text)
        if match_qty_unit:
            # Extraire la quantité numérique
            quantity_match_in_group2 = re.match(r"(\d+[\.,]\d*|\d+/\d+|\d+)", match_qty_unit.group(2).strip())
            if quantity_match_in_group2:
                quantity_str = quantity_match_in_group2.group(1)
            
            unit_str_candidate_from_qty_regex = match_qty_unit.group(3) # L'unité capturée par explicit_units_pattern
            if unit_str_candidate_from_qty_regex: # Si une unité a été capturée par la regex (donc elle est dans explicit_units_pattern)
                unit_str = unit_str_candidate_from_qty_regex.strip()
            
            name_group_match = match_qty_unit.group(4)
            if name_group_match is not None:
                ingredient_name_part = name_group_match.strip()
            else: # Si group(4) est None, cela signifie qu'il n'y avait rien après la quantité/unité
                ingredient_name_part = "" # Le nom de l'ingrédient est vide dans ce cas
        else: # Pas de quantité/unité numérique claire au début, on prend tout comme nom
            ingredient_name_part = text.strip()

    # Nettoyer le nom de l'ingrédient
    # Supprimer "de", "d'" au début si ce n'est pas une unité comme "pot de"
    if ingredient_name_part.startswith("de "):
        ingredient_name_part = ingredient_name_part[3:]
    elif ingredient_name_part.startswith("d'"):
        ingredient_name_part = ingredient_name_part[2:]
    
    # Si ingredient_name_part est vide et qu'on a une unité mais pas de quantité textuelle (ex: "g de sucre" -> "sucre")
    # # Et que l'unité n'est pas une unité textuelle (comme "une pincée de")
    # if not ingredient_name_part.strip() and unit_str and not match_text_unit :
    #     ingredient_name_part = unit_str # L'ancienne "unité" était en fait le nom
    #     unit_str = None # Il n'y avait pas vraiment d'unité
    
    # Enlever les adjectifs et mots communs du nom (moins agressif que normalize_name)
    ingredient_name_part = re.sub(r"\s*\([^)]*\)", "", ingredient_name_part).strip() # parenthèses
    words = ingredient_name_part.split()
    # Mots à potentiellement enlever si en fin de nom (ex: "pommes de terre coupées en dés")
    preparation_terms = ["coupé", "coupée", "coupés", "coupées", "émincé", "émincée", "haché", "hachée", "fondu", "fondue", "frais", "fraîche", "frais", "fraîches", "en dés", "en rondelles", "finement"]
    # Simple nettoyage pour l'instant
    cleaned_name = " ".join(words).strip()

    # Conversion en grammes
    if quantity_str and unit_str and unit_str in UNIT_TO_GRAMS_APPROX:
        try:
            # Gérer les fractions comme "1/2"
            if "/" in quantity_str:
                num, den = map(float, quantity_str.split('/'))
                q_val = num / den
            else:
                q_val = float(quantity_str.replace(",", "."))
            quantity_grams = q_val * UNIT_TO_GRAMS_APPROX[unit_str]
        except ValueError:
            pass # Laisser quantity_grams à None
    elif quantity_str and not unit_str: # Ex: "2 pommes"
        try: # Si c'est juste un nombre, on peut essayer de le multiplier par un poids moyen d'unité si le nom le suggère
            q_val = float(quantity_str.replace(",", "."))
            # Heuristique: si le nom est au pluriel et la quantité est > 1, ou singulier et quantité = 1
            # On pourrait avoir une table de poids moyen par ingrédient, mais c'est complexe.
            # Pour l'instant, on ne fait rien de plus ici, quantity_grams reste None ou DEFAULT_QUANTITY_GRAMS sera utilisé.
            pass
        except ValueError:
            pass

    return {
        "raw_text": original_string,
        "quantity_str": quantity_str,
        "unit_str": unit_str,
        "parsed_name": cleaned_name, # Nom nettoyé pour affichage/logique
        "quantity_grams": quantity_grams if quantity_grams is not None else (DEFAULT_QUANTITY_GRAMS if quantity_str else None) # Default si qte mais pas d'unité claire
    }

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
