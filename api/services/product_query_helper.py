import os
import sys
from typing import List, Optional, Dict, Any, Set
import psycopg2 # type: ignore
from psycopg2.extras import DictCursor # type: ignore
import pymongo # type: ignore


# Assurez-vous que le chemin vers 'processing' est correct pour importer 'utils'
# Cela suppose que 'api' et 'processing' sont des dossiers frères.
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'processing'))
from processing.utils import normalize_name, vectorize_name

# Fonctions pour obtenir les connexions (à adapter si elles sont ailleurs)
# Pour cet exemple, je vais supposer qu'elles sont accessibles ou définies ici.
# Idéalement, elles proviendraient d'un module partagé comme api.db

def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'postgres'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        cursor_factory=DictCursor # Pour obtenir des résultats comme des dictionnaires
    )

def get_mongo_client_connection():
    return pymongo.MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)


def _get_product_vector_ids_by_name(
    conn: psycopg2.extensions.connection,
    normalized_name_search: str,
    min_name_similarity: float
) -> Set[int]:
    """
    Trouve les IDs initiaux de product_vector basés sur une correspondance de nom floue.
    """
    if not normalized_name_search:
        return set()

    ids = set()
    with conn.cursor() as cur:
        try:
            # Utilise pg_trgm pour la similarité de nom
            # Le seuil de similarité est appliqué directement dans la requête
            # Note: Assurez-vous que l'extension pg_trgm est activée et que la colonne name est indexée pour la similarité.
            sql = """
                SELECT id FROM product_vector
                WHERE similarity(name, %s) >= %s
            """
            cur.execute(sql, (normalized_name_search, min_name_similarity))
            for row in cur.fetchall():
                ids.add(row['id']) # type: ignore
        except Exception as e:
            print(f"Error in _get_product_vector_ids_by_name: {e}") # Log error
    return ids


def _get_linked_product_vector_ids(
    conn: psycopg2.extensions.connection,
    initial_ids: Set[int],
    min_similarity_score: float
) -> Dict[int, Dict[str, Any]]: # Retourne un dict: {initial_pv_id: {source_liee: {id: id_lie, name: nom_lie, score: score_lien}}}
    """
    Pour chaque ID initial, trouve le MEILLEUR ID de product_vector lié dans CHAQUE AUTRE source,
    en respectant le min_similarity_score.
    Utilise une logique similaire à find_similar_ingredients mais directement avec psycopg2.
    """
    if not initial_ids:
        return {}

    best_links_per_initial_id: Dict[int, Dict[str, Any]] = {}

    with conn.cursor() as cur:
        try:
            # Récupérer les noms et vecteurs des produits initiaux
            cur.execute(
                "SELECT id, name, source, name_vector FROM product_vector WHERE id = ANY(%s)",
                (list(initial_ids),)
            )
            initial_products_data = {row['id']: row for row in cur.fetchall()} #type: ignore

            # Récupérer toutes les sources distinctes une seule fois
            cur.execute("SELECT DISTINCT source FROM product_vector;")
            all_db_sources = [row['source'] for row in cur.fetchall()] # type: ignore

            for initial_pv_id, initial_product_info in initial_products_data.items():
                current_initial_source = initial_product_info['source'] # type: ignore
                current_initial_name = initial_product_info['name'] # type: ignore
                # current_initial_vector = initial_product_info['name_vector'] # Pas besoin du vecteur ici si on utilise ingredient_link

                best_links_for_current_initial: Dict[str, Any] = {}

                # Option 1: Utiliser la table ingredient_link (plus rapide si elle est bien remplie)
                # Pour chaque autre source, trouver le meilleur lien depuis ingredient_link
                for other_source_in_db in all_db_sources:
                    if other_source_in_db == current_initial_source:
                        continue

                    # Chercher où initial_pv_id est id_source
                    cur.execute("""
                        SELECT il.id_linked, pv.name as linked_name, il.score
                        FROM ingredient_link il
                        JOIN product_vector pv ON il.id_linked = pv.id
                        WHERE il.id_source = %s AND il.linked_source = %s AND il.score >= %s
                        ORDER BY il.score DESC
                        LIMIT 1;
                    """, (initial_pv_id, other_source_in_db, min_similarity_score))
                    match1 = cur.fetchone()

                    # Chercher où initial_pv_id est id_linked
                    cur.execute("""
                        SELECT il.id_source as id_linked, pv.name as linked_name, il.score
                        FROM ingredient_link il
                        JOIN product_vector pv ON il.id_source = pv.id
                        WHERE il.id_linked = %s AND il.source = %s AND il.score >= %s
                        ORDER BY il.score DESC
                        LIMIT 1;
                    """, (initial_pv_id, other_source_in_db, min_similarity_score))
                    match2 = cur.fetchone()
                    
                    best_match_for_source = None
                    if match1 and (not match2 or match1['score'] >= match2['score']): # type: ignore
                        best_match_for_source = match1
                    elif match2:
                        best_match_for_source = match2
                    
                    if best_match_for_source:
                        best_links_for_current_initial[other_source_in_db] = {'id': best_match_for_source['id_linked'], 'name': best_match_for_source['linked_name'], 'score': best_match_for_source['score']} # type: ignore
                
                if best_links_for_current_initial:
                    best_links_per_initial_id[initial_pv_id] = best_links_for_current_initial

        except Exception as e:
            print(f"Error in _get_linked_product_vector_ids: {e}") # Log error
    return best_links_per_initial_id


def _fetch_product_details(
    conn: psycopg2.extensions.connection,
    product_vector_ids: Set[int]
) -> List[Dict[str, Any]]:
    """
    Récupère les données détaillées de product_vector et des tables spécifiques à la source.
    """
    if not product_vector_ids:
        return []

    products_dict: Dict[int, Dict[str, Any]] = {}
    with conn.cursor() as cur:
        try:
            # 1. Récupérer les détails de base de product_vector
            cur.execute(
                "SELECT id, name, source, code_source FROM product_vector WHERE id = ANY(%s)",
                (list(product_vector_ids),)
            )
            for row in cur.fetchall():
                products_dict[row['id']] = dict(row) # type: ignore


            # 2. Récupérer les détails des tables spécifiques
            source_tables_config = {
                'agribalyse': {'table': 'agribalyse'},
                'openfoodfacts': {'table': 'openfoodfacts'},
                'greenpeace': {'table': 'greenpeace_season'}
            }

            for pv_id, product_data in products_dict.items():
                source_name = product_data['source']
                if source_name in source_tables_config:
                    config = source_tables_config[source_name]
                    table_name = config['table']
                    
                    # Pour Greenpeace, les mois sont multiples, donc on les agrège
                    if source_name == 'greenpeace':
                        cur.execute(
                            f"SELECT month FROM {table_name} WHERE product_vector_id = %s",
                            (pv_id,)
                        )
                        months = [row['month'] for row in cur.fetchall()] # type: ignore
                        if months:
                             # Fusionner directement les mois dans l'objet produit
                             products_dict[pv_id]['months_in_season'] = months # Nom de clé plus descriptif
                    else:
                        # Pour les autres sources, on s'attend à une seule ligne de détails
                        cur.execute(
                            f"SELECT * FROM {table_name} WHERE product_vector_id = %s LIMIT 1",
                            (pv_id,)
                        )
                        details_row = cur.fetchone()
                        if details_row:
                            # Fusionner les détails dans l'objet produit principal
                            # Exclure 'id' et 'product_vector_id' de la table source pour éviter les conflits/redondances
                            source_details = {k: v for k, v in dict(details_row).items() if k not in ['id', 'product_vector_id']}
                            products_dict[pv_id].update(source_details)
        
        except Exception as e:
            print(f"Error in _fetch_product_details: {e}")
            return [] # Retourner une liste vide en cas d'erreur majeure
    return list(products_dict.values())


def _calculate_similarity_to_search_term(
    conn: psycopg2.extensions.connection,
    product_vector_id: int,
    normalized_search_name: str,
    search_vector: List[float]
) -> float:
    """
    Calcule le score de similarité combiné (flou + vectoriel) d'un produit
    par rapport au terme de recherche normalisé et à son vecteur.
    """
    score = 0.0
    with conn.cursor() as cur:
        try:
            # Le vecteur doit être passé comme une chaîne de caractères pour psycopg2
            search_vector_str = '[' + ','.join(map(str, search_vector)) + ']'
            sql = """
                SELECT (0.4 * (1 - (pv.name_vector <=> %s::vector)) + 0.6 * similarity(pv.name, %s)) AS global_score
                FROM product_vector pv
                WHERE pv.id = %s
            """
            cur.execute(sql, (search_vector_str, normalized_search_name, product_vector_id))
            row = cur.fetchone()
            if row and row['global_score'] is not None: # type: ignore
                score = float(row['global_score']) # type: ignore
        except Exception as e:
            print(f"Error calculating similarity score for pv_id {product_vector_id}: {e}")
    return score


def _fetch_recipes_for_ingredient(
    mongo_client: pymongo.MongoClient,
    normalized_ingredient_name: str,
    limit: int = 10,
    skip: int = 0
) -> List[Dict[str, Any]]:
    """
    Récupère les recettes de MongoDB qui contiennent le nom d'ingrédient normalisé donné.
    """
    if not mongo_client or not normalized_ingredient_name:
        return []

    recipes_data = []
    try:
        db = mongo_client["OpenFoodImpact"]
        collection = db["recipes"]
        query = {"normalized_ingredients": normalized_ingredient_name}
        
        cursor = collection.find(query, {"_id": 0}).skip(skip).limit(limit)
        recipes_data = list(cursor)
    except Exception as e:
        print(f"Error fetching recipes from MongoDB: {e}")
    
    return recipes_data
