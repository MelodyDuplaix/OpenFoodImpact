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

# mots à éliminer de la normalisation des noms de produits
STOPWORDS = {
    "de", "du", "des", "d'", "la", "le", "les", "l'", "en", "avec", "et", "à", "au", "aux", "un", "une", "-"
}

# unités de mesure et leurs équivalents en grammes
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
    text = ingredient_string.lower().strip()

    quantity_str = None
    unit_str = None
    quantity_grams = None
    ingredient_name_part = text

    # on sépare les quantités collées à l'unité (ex: 250g)
    text = re.sub(r"(\d)([a-zA-Zéèêëàâäîïôöùûüç]+)", r"\1 \2", text)

    # on cherche les unités explicites
    explicit_units_pattern = r"\b(" + "|".join(re.escape(u) for u in UNITES) + r")\b"

    # capture la quantité / fraction / nombre, l'unité si présente, et le nom de l'ingrédient dans la chaine
    regex_qty_unit = rf"^(\d+[\.,]\d*|\d+/\d+|\d+)\s*({explicit_units_pattern})?\b\s*(.*)"
    # capture les unités explicites type cuillères avec un nombre de 1 à 10 en début de chaîne, et le nom de l'ingrédient
    regex_text_unit = r"^(une?|deux|trois|quatre|cinq|six|sept|huit|neuf|dix)\s+([a-zA-Zàâäéèêëïîôöùûüç\s\.\-']+?)\s+(de|d')\s+(.*)"

    match_text_unit = re.match(regex_text_unit, text)
    if match_text_unit:
        # si on trouve une unité explicite au début, on récupère la quantité et l'unité, on vérifie l'unité, et on extrait le nom de l'ingrédient
        quantity_str = match_text_unit.group(1).strip()
        unit_candidate = match_text_unit.group(2).strip()
        if re.fullmatch(explicit_units_pattern, unit_candidate, re.IGNORECASE):
            unit_str = unit_candidate
            ingredient_name_part = match_text_unit.group(4).strip()
        # si ce n'est pas une unité connue, on considère qu'elle fait partie du nom de l'ingrédient
        else:
            unit_str = None
            ingredient_name_part = unit_candidate + " " + match_text_unit.group(4).strip()
    else:
        # si on ne trouve pas d'unité explicite au début, on cherche une quantité suivie d'une unité
        match_qty_unit = re.match(regex_qty_unit, text)
        if match_qty_unit:
            # on capture la quantité et l'unité
            quantity_str = match_qty_unit.group(1).replace(",", ".")
            unit_str = match_qty_unit.group(2).strip() if match_qty_unit.group(2) else None
            # Prendre tout ce qui suit la quantité et l'unité
            ingredient_name_part = match_qty_unit.group(3).strip() if match_qty_unit.group(3) else ""
            # Nettoyer le début (de, d', etc.)
            ingredient_name_part = re.sub(r"^((de|d'|du|des|la|le|l'|aux|au|a|à)\s+)+", "", ingredient_name_part)
            # Si le nom capturé est vide ou une unité, prendre le reste de la chaîne après la quantité et l'unité
            if not ingredient_name_part or ingredient_name_part in UNITES:
                # Reconstituer la chaîne sans la quantité et l'unité
                pattern = rf"^(\d+[\.,]\d*|\d+/\d+|\d+)\s*({explicit_units_pattern})?\b\s*((de|d'|du|des|la|le|l'|aux|au|a|à)\s+)?"
                ingredient_name_part = re.sub(pattern, "", text).strip()
        else:
            ingredient_name_part = text.strip()

    # Nettoyage du nom d'ingrédient : supprimer tous les mots de liaison en début de chaîne
    ingredient_name_part = re.sub(r"^((de|d'|du|des|la|le|l'|aux|au|a|à)\s+)+", "", ingredient_name_part)
    # Enlever parenthèses et leur contenu
    ingredient_name_part = re.sub(r"\s*\([^)]*\)", "", ingredient_name_part).strip()
    # Nettoyer les espaces multiples
    cleaned_name = re.sub(r"\s+", " ", ingredient_name_part).strip()

    # Si le nom est vide ou ne contient qu'un mot de liaison ou une unité, reprendre le texte original sans quantité/unité
    if not cleaned_name or cleaned_name in {"de", "d'", "à", "a", "du", "des", "la", "le", "l'", "aux", "au"} or cleaned_name in UNITES:
        # Reconstituer la chaîne sans la quantité et l'unité
        pattern = rf"^(\d+[\.,]\d*|\d+/\d+|\d+)\s*({explicit_units_pattern})?\b\s*((de|d'|du|des|la|le|l'|aux|au|a|à)\s+)?"
        cleaned_name = re.sub(pattern, "", text).strip()
        cleaned_name = re.sub(r"^((de|d'|du|des|la|le|l'|aux|au|a|à)\s+)+", "", cleaned_name)
        cleaned_name = re.sub(r"\s+", " ", cleaned_name).strip()

    # on convertit en grammes en fonction de l'unité
    if quantity_str and unit_str and unit_str in UNIT_TO_GRAMS_APPROX:
        try:
            if "/" in quantity_str:
                num, den = map(float, quantity_str.split('/'))
                q_val = num / den
            else:
                q_val = float(quantity_str)
            quantity_grams = q_val * UNIT_TO_GRAMS_APPROX[unit_str]
        except Exception:
            pass
    elif quantity_str and not unit_str:
        try:
            q_val = float(quantity_str)
            quantity_grams = q_val
        except Exception:
            pass

    if quantity_str is None and unit_str is None:
        quantity_str = "1"
        PIECE_KEYWORDS = [
            "pain", "avocat", "oeuf", "banane", "pomme", "poire", "orange", "citron", "tomate", "carotte", "courgette", "aubergine",
            "poivron", "oignon", "échalote", "ail", "figue", "abricot", "prune", "cerise", "fraise", "framboise", "myrtille",
            "raisin", "kiwi", "melon", "pastèque", "ananas", "mangue", "papaye", "litchi", "noix", "noisette", "amande",
            "pistache", "châtaigne", "champignon", "pêche", "nectarine", "grenade", "clementine", "mandarine", "patate", "pomme de terre",
            "navet", "radis", "betterave", "brocoli", "chou", "salade", "laitue", "endive", "épinard", "poireau", "haricot",
            "petit pois", "maïs", "concombre", "courge", "potiron", "citrouille", "salsifis", "topinambour", "artichaut", "asperge",
            "biscuit", "steak", "filet", "saucisse", "poulet", "cuisse", "aileron", "magret", "côtelette", "boule", "branche"
        ]
        unit_str = "pièce" if any(mot in cleaned_name for mot in PIECE_KEYWORDS) else None
        if unit_str and unit_str in UNIT_TO_GRAMS_APPROX:
            quantity_grams = UNIT_TO_GRAMS_APPROX[unit_str]
        else:
            quantity_grams = DEFAULT_QUANTITY_GRAMS


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


if __name__ == "__main__":
    # ingrédients de tests extrait de la base de données mongodb
    ingredients_test = [
    '500 g de penne (ou toutes autres grosses pâtes)',
    '1 Magrets de Canard ou filet',
    '3 boules de sorbet mangue',
    '100 g de lardons de volaille ou de canard',
    'dé de mimolette',
    "2 tasses d'eau citronnée",
    "cuillère à soupe d'huile végétale (noix, ou raisin)",
    '2 pommes de terre (moyenne a grosse)',
    '6 figues mûres mais fermes',
    '1 sachets de gruyère râpé 200 g',
    '30 ml de farine tout usage',
    '150 g de sucre (plutôt roux)',
    '10 cl de vin blanc ou de fond de veau',
    '550 g de patate douce',
    '1 steaks de bœuf',
    '200 g de fruits du jacquier en boîte ou sous vide rincé et égoutté',
    'zeste de citron (ou vanille bourbon)',
    'poivre (noir)',
    '4 cl de rhum ou de cognac',
    '60 g de raisins secs',
    '150 g de lard',
    '65 g de beurre (à température ambiante)',
    '300 g de poisson à chair noire ou blanche (julienne, lieu noir...)',
    '2 petites branches de céleri',
    '80 g de tomates grappes',
    '500 g de brocoli divisés en petits bouquets',
    'poivre noir du moulin, sel',
    '30 croûtons frits aillés',
    '325 g de beurre',
    '35 g de sucre semoule',
    '150 g de mozzarella',
    '2 kg de prune',
    '20 g de laitue',
    '10 tranches de fromage à raclette',
    '2 kg de boeuf (paleron, jarret ou gîte à la noix)',
    '200 g de jambon',
    '12 g de poivre',
    'biscuit thé de lu',
    '500 g de framboises fraîches',
    '50 g de beurre à température ambiante',
    "1 gouttes d'extrait de vanille",
    'avocat',
    '100 g de comté rapé',
    '1 cuillères de coriandre frais haché',
    '1 café de levure',
    '200 g de polenta',
    '125 g de chocolat noir à dessert',
    "3 poivrons (1 de chaque couleur, c'est plus joli)",
    '3 demis de poire au sirop',
    '25 g de beurre fondu',
    '150 g de chocolat au caramel',
    'cuillère à café de fumet de poisson et d’1/2 cuillère à café de bouillon de légumes émietté',
    "1 bonnes poignées d'arachide",
    '6 cuillères à soupe de cacao en poudre',
    '1 pincées de cumin',
    '3 cuillères à soupe de sirop à la vanille',
    '500 g de poireau coupés fin',
    '1 boîtes de concentré de tomates (140 g)',
    '20 cl de vin blanc doux',
    '255 g de chocolat noir',
    '1 rouleaux de pâte feuilletée',
    '1 reblochons au lait cru',
    "600 g d'abricot",
    '1 fond de tarte sablée, cuit à blanc (se trouve également en vente déjà cuit, rayon pâtisserie)',
    'pain de mie',
    '25 cl de vin blanc doux',
    "400 g de lard de poitrine frais avec l'os (ou à défaut 200g de lardons)",
    '2.5 cl de lait',
    '2 kg de boeuf (tous les morceaux conviennent, mais préférer les os)',
    '2 cuillères de concentré de tomates',
    '250 g de vermicelles de riz'
    ]

    for ingredient in ingredients_test:
        result = parse_ingredient_details_fr_en(ingredient)
        print(result)