import os
import sys
from typing import List, Optional, Dict, Any, Set
import psycopg2 # type: ignore
from psycopg2.extras import DictCursor # type: ignore
import pymongo # type: ignore
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'processing'))
from processing.utils import normalize_name, vectorize_name
from processing.utils import DEFAULT_QUANTITY_GRAMS # Importer la constante
def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'postgres'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        cursor_factory=DictCursor
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
            sql = """
                SELECT id FROM product_vector
                WHERE similarity(name, %s) >= %s
            """
            cur.execute(sql, (normalized_name_search, min_name_similarity))
            for row in cur.fetchall():
                ids.add(row['id']) # type: ignore
        except Exception as e:
            print(f"Error in _get_product_vector_ids_by_name: {e}")
    return ids

def _get_linked_product_vector_ids(
    conn: psycopg2.extensions.connection,
    initial_ids: Set[int],
    min_similarity_score: float
) -> Dict[int, Dict[str, Any]]: # Retourne un dict: {initial_pv_id: {source_liee: {id: id_lie, name: nom_lie, score: score_lien}}}
    """
    Pour chaque ID initial, trouve le MEILLEUR ID de product_vector lié dans CHAQUE AUTRE source,
    """
    if not initial_ids:
        return {}

    best_links_per_initial_id: Dict[int, Dict[str, Any]] = {}

    with conn.cursor() as cur:
        try:
            cur.execute(
                "SELECT id, name, source, name_vector FROM product_vector WHERE id = ANY(%s)",
                (list(initial_ids),)
            )
            initial_products_data = {row['id']: row for row in cur.fetchall()} #type: ignore

            cur.execute("SELECT DISTINCT source FROM product_vector;")
            all_db_sources = [row['source'] for row in cur.fetchall()] # type: ignore

            for initial_pv_id, initial_product_info in initial_products_data.items():
                current_initial_source = initial_product_info['source'] # type: ignore
                current_initial_name = initial_product_info['name'] # type: ignore
                best_links_for_current_initial: Dict[str, Any] = {}

                for other_source_in_db in all_db_sources:
                    if other_source_in_db == current_initial_source:
                        continue

                    # Requête combinée pour trouver le meilleur lien dans les deux sens
                    cur.execute("""
                        WITH potential_links AS (
                            (SELECT il.id_linked, pv.name as linked_name, il.score
                             FROM ingredient_link il
                             JOIN product_vector pv ON il.id_linked = pv.id
                             WHERE il.id_source = %s AND il.linked_source = %s AND il.score >= %s
                             ORDER BY il.score DESC
                             LIMIT 1)
                            UNION ALL
                            (SELECT il.id_source as id_linked, pv.name as linked_name, il.score
                             FROM ingredient_link il
                             JOIN product_vector pv ON il.id_source = pv.id
                             WHERE il.id_linked = %s AND il.source = %s AND il.score >= %s
                             ORDER BY il.score DESC
                             LIMIT 1)
                        )
                        SELECT id_linked, linked_name, score
                        FROM potential_links
                        ORDER BY score DESC
                        LIMIT 1;
                    """, (initial_pv_id, other_source_in_db, min_similarity_score,
                          initial_pv_id, other_source_in_db, min_similarity_score))
                    
                    best_match_for_source = cur.fetchone()
                    
                    if best_match_for_source:
                        best_links_for_current_initial[other_source_in_db] = {
                            'id': best_match_for_source['id_linked'], # type: ignore
                            'name': best_match_for_source['linked_name'], # type: ignore
                            'score': best_match_for_source['score'] # type: ignore
                        }
                
                if best_links_for_current_initial:
                    best_links_per_initial_id[initial_pv_id] = best_links_for_current_initial
        except Exception as e:
            print(f"Error in _get_linked_product_vector_ids: {e}")
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
            cur.execute(
                "SELECT id, name, source, code_source FROM product_vector WHERE id = ANY(%s)",
                (list(product_vector_ids),)
            )
            for row in cur.fetchall():
                products_dict[row['id']] = dict(row) # type: ignore
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
                    if source_name == 'greenpeace':
                        cur.execute(
                            f"SELECT month FROM {table_name} WHERE product_vector_id = %s",
                            (pv_id,)
                        )
                        months = [row['month'] for row in cur.fetchall()] # type: ignore
                        if months:
                             products_dict[pv_id]['months_in_season'] = months
                    else:
                        cur.execute(
                            f"SELECT * FROM {table_name} WHERE product_vector_id = %s LIMIT 1",
                            (pv_id,)
                        )
                        details_row = cur.fetchone()
                        if details_row:
                            source_details = {k: v for k, v in dict(details_row).items() if k not in ['id', 'product_vector_id']}
                            products_dict[pv_id].update(source_details)
        except Exception as e:
            print(f"Error in _fetch_product_details: {e}")
            return []
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


def _get_processed_products(
    conn: psycopg2.extensions.connection,
    all_unique_pv_ids_to_fetch: Set[int],
    normalized_search_name: str,
    search_vector: List[float]
) -> List[Dict[str, Any]]:
    """
    Récupère les détails des produits, calcule les scores de similarité,
    filtre pour le meilleur produit par source et trie la liste finale.
    """
    if not all_unique_pv_ids_to_fetch:
        return []

    products_with_details = _fetch_product_details(conn, all_unique_pv_ids_to_fetch)

    for product in products_with_details:
        product['score_to_search'] = _calculate_similarity_to_search_term(
            conn, product['id'], normalized_search_name, search_vector
        )
    
    best_product_per_source: Dict[str, Any] = {}
    products_with_details.sort(key=lambda x: (x.get('source'), x.get('score_to_search', 0.0)), reverse=True)

    for product in products_with_details:
        source = product['source']
        current_score = product.get('score_to_search', 0.0)
        if source not in best_product_per_source or current_score > best_product_per_source[source].get('score_to_search', 0.0):
            best_product_per_source[source] = product
    
    final_products_list = list(best_product_per_source.values())
    final_products_list.sort(key=lambda x: x.get('score_to_search', 0.0), reverse=True)
    
    return final_products_list


def _aggregate_product_details(
    final_products_list: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Agrège les détails des produits à partir de la liste finale des produits,
    en gérant les conflits de clés en préfixant avec la source.
    """
    global_details_aggregator: Dict[str, Any] = {}
    excluded_keys_for_global_details = [
        'id', 'name', 'source', 'code_source', 'score_to_search', 'name_vector',
        'product_name', 'code',
        'nom_produit_francais', 'code_agb', 'code_ciqual', 'lci_name'
    ]

    for product in final_products_list:
        for key, value in product.items():
            if key not in excluded_keys_for_global_details and value is not None:
                if key in global_details_aggregator and global_details_aggregator[key] != value:
                    global_details_aggregator[f"{product['source']}_{key}"] = value
                elif key not in global_details_aggregator:
                    global_details_aggregator[key] = value
    return global_details_aggregator

def _get_details_for_single_ingredient(
    pg_conn: psycopg2.extensions.connection,
    ingredient_name: str,
    min_linked_similarity_score: float,
    min_initial_name_similarity: float
) -> Dict[str, Any]:
    """
    Récupère les détails agrégés pour UN SEUL ingrédient en cherchant ses
    correspondances dans product_vector et en agrégeant les infos des sources liées.
    """
    search_vector = vectorize_name(ingredient_name)
    initial_pv_ids = _get_product_vector_ids_by_name(pg_conn, ingredient_name, min_initial_name_similarity)
    if not initial_pv_ids:
        return {}

    best_links_map = _get_linked_product_vector_ids(pg_conn, initial_pv_ids, min_linked_similarity_score)

    all_unique_pv_ids_to_fetch = set(initial_pv_ids)
    for initial_id in best_links_map:
        for linked_source_data in best_links_map[initial_id].values():
            all_unique_pv_ids_to_fetch.add(linked_source_data['id'])

    if not all_unique_pv_ids_to_fetch:
        pass


    processed_products_for_ingredient = _get_processed_products(
        pg_conn,
        all_unique_pv_ids_to_fetch,
        ingredient_name,
        search_vector
    )

    if not processed_products_for_ingredient:
        return {}

    ingredient_aggregated_details = _aggregate_product_details(processed_products_for_ingredient)
    # Ajouter le nom original normalisé pour lequel la recherche a été faite, pour le mapping futur
    ingredient_aggregated_details["original_normalized_search_name"] = ingredient_name
    return ingredient_aggregated_details


def _aggregate_details_for_recipe(
    ingredient_details_cache: Dict[str, Dict[str, Any]], # Cache des détails par nom normalisé
    recipe_parsed_ingredients: List[Dict[str, Any]] # Vient de la recette MongoDB, avec qtés parsées
) -> Dict[str, Any]:
    """
    Agrège les détails de plusieurs ingrédients pour obtenir les détails globaux d'une recette.
    Les valeurs nutritionnelles et environnementales sont sommées en pondérant par les quantités.
    'months_in_season' est une intersection/union des mois disponibles.
    """
    recipe_details: Dict[str, Any] = {}
    summable_fields = [
        "energy_kcal_100g", "fat_100g", "saturated_fat_100g", "carbohydrates_100g",
        "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
        "changement_climatique", "score_unique_ef", "ecotoxicite_eau_douce",
        "epuisement_ressources_energetiques", "eutrophisation_marine",
        "effets_tox_cancerogenes", "epuisement_ressources_eau", "eutrophisation_terrestre",
        "utilisation_sol", "effets_tox_non_cancerogenes", "epuisement_ressources_mineraux",
        "particules_fines", "formation_photochimique_ozone", "changement_climatique_biogenique",
        "acidification_terrestre_eaux_douces", "changement_climatique_cas",
        "appauvrissement_couche_ozone", "rayonnements_ionisants", "eutrophisation_eaux_douces",
        "changement_climatique_fossile"
    ]

    for field in summable_fields:
        recipe_details[field] = 0.0

    all_months_lists: List[List[str]] = []
    processed_ingredients_with_details_count = 0
    # total_recipe_weight_g = 0 # Si on veut normaliser par 100g de recette totale

    for ing_from_recipe in recipe_parsed_ingredients:
        # ing_from_recipe est un dict de parse_ingredient_details_fr_en
        # ex: {"raw_text": "2 pommes", "quantity_str": "2", ..., 
        #      "parsed_name": "pommes", "quantity_grams": 260, 
        #      "normalized_name_for_matching": "pomme"}
        
        normalized_name_key = ing_from_recipe.get("normalized_name_for_matching")
        if not normalized_name_key:
            continue

        ing_details_from_cache = ingredient_details_cache.get(normalized_name_key)
        if not ing_details_from_cache or not isinstance(ing_details_from_cache, dict): # Peut être {}
            continue
        
        processed_ingredients_with_details_count += 1
        
        # Utiliser la quantité en grammes parsée, ou une valeur par défaut si non disponible mais que l'ingrédient est listé
        quantity_grams = ing_from_recipe.get("quantity_grams")
        if quantity_grams is None: # Si parse_ingredient_details_fr_en n'a pas pu déterminer de grammes
            # Si une quantity_str existe (ex: "1" pour "1 oignon"), on pourrait utiliser DEFAULT_QUANTITY_GRAMS
            # Sinon, si pas de quantity_str, on pourrait ignorer ou utiliser un poids très faible.
            # Pour l'instant, si quantity_grams est None, on utilise DEFAULT_QUANTITY_GRAMS
            # si l'ingrédient semble avoir une quantité implicite (ex: "sel" vs "1 oignon")
            # C'est complexe. Simplifions : si quantity_grams est None, on prend DEFAULT_QUANTITY_GRAMS.
            quantity_grams = DEFAULT_QUANTITY_GRAMS

        # total_recipe_weight_g += quantity_grams

        for field in summable_fields:
            value_per_100g = ing_details_from_cache.get(field)
            if isinstance(value_per_100g, (int, float)):
                recipe_details[field] += (value_per_100g / 100.0) * quantity_grams
        
        if "months_in_season" in ing_details_from_cache and isinstance(ing_details_from_cache["months_in_season"], list):
            all_months_lists.append(ing_details_from_cache["months_in_season"])

    if all_months_lists:
        common_months = set(all_months_lists[0])
        for month_list in all_months_lists[1:]:
            common_months.intersection_update(month_list)
        if common_months:
            recipe_details["months_in_season"] = sorted(list(common_months))
        else: # Pas de mois en commun, on prend l'union de tous les mois
            union_months = set()
            for month_list in all_months_lists:
                union_months.update(month_list)
            recipe_details["months_in_season"] = sorted(list(union_months))
    elif processed_ingredients_with_details_count > 0:
        recipe_details["months_in_season"] = []

    for field in summable_fields:
        if isinstance(recipe_details[field], float):
            recipe_details[field] = round(recipe_details[field], 3)

    if processed_ingredients_with_details_count == 0:
        return {}
        
    return recipe_details


def get_enriched_recipes_details(
    pg_conn: psycopg2.extensions.connection,
    recipes: List[Dict[str, Any]],
    min_linked_similarity_score: float,
    min_initial_name_similarity: float
) -> List[Dict[str, Any]]:
    """
    Enrichit une liste de recettes avec les détails agrégés de leurs ingrédients.
    """
    # 1. Collecter tous les ingrédients normalisés uniques (normalized_name_for_matching)
    #    à partir des `parsed_ingredients_details` de toutes les recettes demandées.
    all_unique_normalized_ingredients_to_fetch = set()
    for recipe in recipes:
        # Le champ recipe.get("parsed_ingredients_details") doit exister suite au parsing lors de l'ETL
        parsed_ingredients_list = recipe.get("parsed_ingredients_details", [])
        if parsed_ingredients_list:
            for ing_detail in parsed_ingredients_list:
                # ing_detail est un dict comme retourné par parse_ingredient_details_fr_en
                normalized_name_key = ing_detail.get("normalized_name_for_matching")
                if normalized_name_key:
                    all_unique_normalized_ingredients_to_fetch.add(normalized_name_key)

    # 2. Récupérer les détails pour chaque ingrédient unique et les mettre en cache
    #    La clé du cache sera `normalized_name_for_matching`.
    ingredient_details_cache: Dict[str, Dict[str, Any]] = {}
    if pg_conn and all_unique_normalized_ingredients_to_fetch:
        for ing_key_name in all_unique_normalized_ingredients_to_fetch:
            details = _get_details_for_single_ingredient(
                pg_conn,
                ing_key_name, # C'est ce nom qui est cherché dans product_vector
                min_linked_similarity_score,
                min_initial_name_similarity
            )
            ingredient_details_cache[ing_key_name] = details # details contient "original_normalized_search_name"

    # 3. Enrichir chaque recette en utilisant le cache
    enriched_recipes_list = []
    for recipe in recipes:
        current_recipe_enriched = recipe.copy()
        # Récupérer les ingrédients parsés de la recette (qui contiennent les quantités)
        recipe_parsed_ingredients = current_recipe_enriched.get("parsed_ingredients_details", [])

        if recipe_parsed_ingredients and ingredient_details_cache:
            # Passer le cache complet et la liste des ingrédients parsés de CETTE recette
            current_recipe_enriched["aggregated_details"] = _aggregate_details_for_recipe(
                ingredient_details_cache,
                recipe_parsed_ingredients
            )
        else:
            current_recipe_enriched["aggregated_details"] = {} 
            
        enriched_recipes_list.append(current_recipe_enriched)
    return enriched_recipes_list
