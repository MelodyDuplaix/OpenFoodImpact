import os
import sys
from time import time
from typing import List, Optional, Dict, Any, Set
import pymongo # type: ignore
from sqlalchemy.orm import Session
from sqlalchemy import func, select, text, or_, and_, case
from sqlalchemy.dialects.postgresql import ARRAY
import logging
logger = logging.getLogger(__name__)

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'processing'))
from processing.utils import normalize_name, vectorize_name
from processing.utils import DEFAULT_QUANTITY_GRAMS
from api.sql_models import ProductVector, IngredientLink, Agribalyse, OpenFoodFacts, GreenpeaceSeason


def _get_product_vector_ids_by_name(
    db: Session,
    normalized_name_search: str,
    min_name_similarity: float
) -> Set[int]:
    """
    Récupère les IDs de product_vector par similarité de nom.

    Args:
        db: Session SQLAlchemy.
        normalized_name_search: Nom normalisé pour la recherche.
        min_name_similarity: Score de similarité de nom minimal.
    Returns:
        Set[int]: Ensemble d'IDs de product_vector correspondants.
    """
    if not normalized_name_search:
        return set()

    ids = set()
    try:
        stmt = (
            select(ProductVector.id)
            .where(func.similarity(ProductVector.name, normalized_name_search) >= min_name_similarity)
        )
        result = db.execute(stmt).scalars().all()
        ids = set(result)
    except Exception as e:
        logger.error(f"Error in _get_product_vector_ids_by_name for '{normalized_name_search}': {e}")
    return ids

def _get_linked_product_vector_ids(
    db: Session,
    initial_ids: Set[int],
    min_similarity_score: float
) -> Dict[int, Dict[str, Any]]:
    """
    Trouve les meilleurs IDs de product_vector liés pour chaque ID initial.

    Args:
        db: Session SQLAlchemy.
        initial_ids: Ensemble d'IDs de product_vector initiaux.
        min_similarity_score: Score de similarité minimal pour les liens.
    Returns:
        Dict[int, Dict[str, Any]]: Dictionnaire des meilleurs liens par ID initial.
        Le format est {initial_pv_id: {source_liee: {id: id_lie, name: nom_lie, score: score_lien}}}.
    """
    if not initial_ids:
        return {}

    best_links_per_initial_id: Dict[int, Dict[str, Any]] = {}

    try:
        # on récupère les données des produits initiaux
        initial_products_data_query = (
            select(ProductVector.id, ProductVector.name, ProductVector.source, ProductVector.name_vector)
            .where(ProductVector.id.in_(initial_ids))
        )
        initial_products_results = db.execute(initial_products_data_query).all()
        initial_products_data = {row.id: row for row in initial_products_results}

        all_db_sources_query = select(ProductVector.source).distinct()
        all_db_sources = [row[0] for row in db.execute(all_db_sources_query).all()]

        for initial_pv_id, initial_product_info in initial_products_data.items():
            # on itère sur chaque produit initial, et on cherche les meilleurs liens pour chaque source
            current_initial_source = initial_product_info.source
            best_links_for_current_initial: Dict[str, Any] = {}

            for other_source_in_db in all_db_sources:
                if other_source_in_db == current_initial_source:
                    continue

                stmt1 = (
                    select(IngredientLink.id_linked, ProductVector.name.label("linked_name"), IngredientLink.score)
                    .join(ProductVector, IngredientLink.id_linked == ProductVector.id)
                    .where(
                        IngredientLink.id_source == initial_pv_id,
                        IngredientLink.linked_source == other_source_in_db,
                        IngredientLink.score >= min_similarity_score
                    )
                    .order_by(IngredientLink.score.desc())
                    .limit(1)
                )

                stmt2 = (
                    select(IngredientLink.id_source.label("id_linked"), ProductVector.name.label("linked_name"), IngredientLink.score)
                    .join(ProductVector, IngredientLink.id_source == ProductVector.id)
                    .where(
                        IngredientLink.id_linked == initial_pv_id,
                        IngredientLink.source == other_source_in_db,
                        IngredientLink.score >= min_similarity_score
                    )
                    .order_by(IngredientLink.score.desc())
                    .limit(1)
                )
                # ces requêtes permettent de récupérer les produits les plus liés au produit initial via les liens d'ingrédients
                res1 = db.execute(stmt1).first()
                res2 = db.execute(stmt2).first()

                best_match_for_source = None
                if res1 and res2:
                    best_match_for_source = res1 if res1.score >= res2.score else res2
                elif res1:
                    best_match_for_source = res1
                elif res2:
                    best_match_for_source = res2
                
                if best_match_for_source:
                    best_links_for_current_initial[other_source_in_db] = {
                        'id': best_match_for_source.id_linked,
                        'name': best_match_for_source.linked_name,
                        'score': best_match_for_source.score
                    }
            
            if best_links_for_current_initial:
                best_links_per_initial_id[initial_pv_id] = best_links_for_current_initial
    except Exception as e:
        logger.error(f"Error in _get_linked_product_vector_ids for initial_ids {initial_ids}: {e}")
    return best_links_per_initial_id


def _fetch_product_details(
    db: Session,
    product_vector_ids: Set[int]
) -> List[Dict[str, Any]]:
    """
    Récupère les détails des produits depuis product_vector et les tables sources en utilisant SQLAlchemy.

    Args:
        db: Session SQLAlchemy.
        product_vector_ids: Ensemble d'IDs de product_vector à récupérer.
    Returns:
        List[Dict[str, Any]]: Liste de dictionnaires contenant les détails des produits.
    """
    if not product_vector_ids:
        return []

    results = []
    try:
        products = (
            db.query(ProductVector)
            .options(
            )
            .filter(ProductVector.id.in_(list(product_vector_ids)))
            .all()
        )

        for pv_item in products:
            product_data = {
                "id": pv_item.id,
                "name": pv_item.name,
                "source": pv_item.source,
            }

            if pv_item.source == 'agribalyse' and pv_item.agribalyse_entries: # type: ignore
                ag_entry = pv_item.agribalyse_entries[0]
                product_data.update({
                    col.name: getattr(ag_entry, col.name)
                    for col in Agribalyse.__table__.columns
                    if col.name not in ['id', 'product_vector_id']
                })
            elif pv_item.source == 'openfoodfacts' and pv_item.openfoodfacts_entries: # type: ignore
                off_entry = pv_item.openfoodfacts_entries[0]
                product_data.update({
                    col.name: getattr(off_entry, col.name)
                    for col in OpenFoodFacts.__table__.columns
                    if col.name not in ['id', 'product_vector_id']
                })
            elif pv_item.source == 'greenpeace' and pv_item.greenpeace_season_entries: # type: ignore
                months = [entry.month for entry in pv_item.greenpeace_season_entries]
                if months:
                    product_data['months_in_season'] = months
            
            results.append(product_data)
            
    except Exception as e:
        logger.error(f"Error in _fetch_product_details for product_vector_ids {product_vector_ids}: {e}")
        return []
    return results


def _calculate_similarity_to_search_term(
    db: Session,
    product_vector_id: int,
    normalized_search_name: str,
    search_vector: List[float]
) -> float:
    """
    Calcule le score de similarité combiné d'un produit par rapport à un terme de recherche.

    Args:
        db: Session SQLAlchemy.
        product_vector_id: ID du produit dans product_vector.
        normalized_search_name: Nom de recherche normalisé.
        search_vector: Vecteur du nom de recherche (list or np.array).
    Returns:
        float: Score de similarité global.
    """
    score = 0.0
    try:
        
        stmt = (
            select(
                (0.4 * (1 - ProductVector.name_vector.cosine_distance(search_vector)) +
                 0.6 * func.similarity(ProductVector.name, normalized_search_name)).label("global_score")
            )
            .where(ProductVector.id == product_vector_id)
        )
        # on calcule le score de similarité combiné entre le vecteur du nom du produit et le nom de recherche via une combinaison pondérée vectorisation + similarité textuelle
        result = db.execute(stmt).scalar_one_or_none()
        if result is not None:
            score = float(result)
    except Exception as e:
        logger.error(f"Error calculating similarity score for pv_id {product_vector_id} against '{normalized_search_name}': {e}")
    return score


def _fetch_recipes_for_ingredient(
    mongo_client: pymongo.MongoClient,
    normalized_ingredient_name: str,
    limit: int = 10,
    skip: int = 0
) -> List[Dict[str, Any]]:
    """
    Récupère les recettes MongoDB contenant un ingrédient normalisé spécifique.

    Args:
        mongo_client: Client MongoDB.
        normalized_ingredient_name: Nom d'ingrédient normalisé à rechercher.
        limit: Nombre maximum de recettes à retourner.
        skip: Nombre de recettes à sauter.
    Returns:
        List[Dict[str, Any]]: Liste des recettes correspondantes.
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
        logger.error(f"Error fetching recipes from MongoDB for ingredient '{normalized_ingredient_name}': {e}")
    
    return recipes_data


def _get_processed_products(
    db: Session,
    all_unique_pv_ids_to_fetch: Set[int],
    normalized_search_name: str,
    search_vector: List[float]
) -> List[Dict[str, Any]]:
    """
    Traite une liste d'IDs de produits pour obtenir une liste finale de produits enrichis et triés.

    Args:
        db: Session SQLAlchemy.
        all_unique_pv_ids_to_fetch: Ensemble d'IDs de product_vector uniques à traiter.
        normalized_search_name: Nom de recherche normalisé.
        search_vector: Vecteur du nom de recherche.
    Returns:
        List[Dict[str, Any]]: Liste finale des produits traités et triés.
    """
    if not all_unique_pv_ids_to_fetch:
        return []
    
    logger.debug(f"_get_processed_products: Fetching details for {len(all_unique_pv_ids_to_fetch)} IDs.")

    # on récupère les détails des produits liés
    products_with_details = _fetch_product_details(db, all_unique_pv_ids_to_fetch)

    # on calcule le score de similarité pour chaque produit
    for product in products_with_details:
        product['score_to_search'] = _calculate_similarity_to_search_term(
            db, product['id'], normalized_search_name, search_vector
        )
    
    best_product_per_source: Dict[str, Any] = {}
    # on trie les produits par source et par score de similarité
    products_with_details.sort(key=lambda x: (x.get('source'), x.get('score_to_search', 0.0)), reverse=True)
    
    logger.debug("_get_processed_products, etape 4")

    for product in products_with_details:
        # on garde le meilleur produit par source
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
    Agrège les détails d'une liste de produits en un seul dictionnaire.

    Args:
        final_products_list: Liste des produits finaux avec leurs détails.
    Returns:
        Dict[str, Any]: Dictionnaire global agrégeant les détails des produits.
        Les clés conflictuelles sont préfixées par la source du produit.
    """
    global_details_aggregator: Dict[str, Any] = {}
    excluded_keys_for_global_details = [
        'id', 'name', 'source', 'score_to_search', 'name_vector',
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
    db: Session,
    ingredient_name: str,
    min_linked_similarity_score: float,
    min_initial_name_similarity: float
) -> Dict[str, Any]:
    """
    Récupère et agrège les détails pour un seul ingrédient, en utilisant les liens précalculés dans ingredient_link.

    Args:
        db: Session SQLAlchemy.
        ingredient_name: Nom de l'ingrédient (normalisé) à rechercher.
        min_linked_similarity_score: Score de similarité minimal pour les produits liés.
        min_initial_name_similarity: Score de similarité minimal pour la recherche initiale du nom.

    Returns:
        Dict[str, Any]: Dictionnaire des détails agrégés pour l'ingrédient.
    """
    logger.debug(f"Getting details for single ingredient: {ingredient_name}")

    initial_pv_ids = _get_product_vector_ids_by_name(db, ingredient_name, min_initial_name_similarity)
    if not initial_pv_ids:
        logger.debug(f"No initial product_vector IDs found for {ingredient_name} with similarity {min_initial_name_similarity}")
        return {"original_normalized_search_name": ingredient_name} 

    all_pv_ids_to_consider = set(initial_pv_ids)
    logger.debug(f"Initial PV IDs for {ingredient_name}: {initial_pv_ids}")
    stmt_linked_from_source = (
        select(IngredientLink.id_linked)
        .where(
            IngredientLink.id_source.in_(initial_pv_ids),
            IngredientLink.score >= min_linked_similarity_score
        )
    )
    linked_ids_from_source = db.execute(stmt_linked_from_source).scalars().all()
    all_pv_ids_to_consider.update(linked_ids_from_source)
    stmt_linked_to_target = (
        select(IngredientLink.id_source) 
        .where(
            IngredientLink.id_linked.in_(initial_pv_ids),
            IngredientLink.score >= min_linked_similarity_score
        )
    )
    linked_ids_to_target = db.execute(stmt_linked_to_target).scalars().all()
    all_pv_ids_to_consider.update(linked_ids_to_target)
    product_details_list = _fetch_product_details(db, all_pv_ids_to_consider)
    logger.debug(f"Fetched details for {len(product_details_list)} products for {ingredient_name}")
    ingredient_aggregated_details = _aggregate_product_details(product_details_list) if product_details_list else {}
    ingredient_aggregated_details["original_normalized_search_name"] = ingredient_name
    return ingredient_aggregated_details


def _aggregate_details_for_recipe(
    ingredient_details_cache: Dict[str, Dict[str, Any]],
    recipe_parsed_ingredients: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Agrège les détails des ingrédients d'une recette pour obtenir ses détails globaux.

    Args:
        ingredient_details_cache: Cache des détails d'ingrédients (clé: nom normalisé).
        recipe_parsed_ingredients: Liste des ingrédients parsés de la recette (avec quantités).
    Returns:
        Dict[str, Any]: Dictionnaire des détails agrégés pour la recette.
        Les valeurs sont pondérées par les quantités d'ingrédients.
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

    for ing_from_recipe in recipe_parsed_ingredients:
        normalized_name_key = ing_from_recipe.get("normalized_name_for_matching")
        if not normalized_name_key:
            continue

        ing_details_from_cache = ingredient_details_cache.get(normalized_name_key)
        if not ing_details_from_cache or not isinstance(ing_details_from_cache, dict):
            continue
        
        processed_ingredients_with_details_count += 1
        
        quantity_grams = ing_from_recipe.get("quantity_grams")
        if quantity_grams is None:
            quantity_grams = DEFAULT_QUANTITY_GRAMS

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
        if common_months: # type: ignore
            recipe_details["months_in_season"] = sorted(list(common_months))
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
    db: Session,
    recipes: List[Dict[str, Any]],
    min_linked_similarity_score: float,
    min_initial_name_similarity: float
) -> List[Dict[str, Any]]:
    """
    Enrichit une liste de recettes avec les détails agrégés de leurs ingrédients.

    Args:
        db: Session SQLAlchemy.
        recipes: Liste de recettes (dictionnaires) à enrichir.
        min_linked_similarity_score: Score de similarité minimal pour les produits liés.
        min_initial_name_similarity: Score de similarité minimal pour la recherche initiale du nom.
    Returns:
        List[Dict[str, Any]]: Liste des recettes enrichies.
    """
    logger.debug("Starting enrichment for multiple recipes.")
    all_unique_normalized_ingredients_to_fetch = set()
    for recipe in recipes:
        parsed_ingredients_list = recipe.get("parsed_ingredients_details", [])
        if parsed_ingredients_list:
            for ing_detail in parsed_ingredients_list:
                normalized_name_key = ing_detail.get("normalized_name_for_matching")
                if normalized_name_key:
                    all_unique_normalized_ingredients_to_fetch.add(normalized_name_key)
                    
    logger.debug(f"Found {len(all_unique_normalized_ingredients_to_fetch)} unique normalized ingredients to fetch details for.")
    ingredient_details_cache: Dict[str, Dict[str, Any]] = {}
    if db and all_unique_normalized_ingredients_to_fetch:
        for ing_key_name in all_unique_normalized_ingredients_to_fetch:
            details = _get_details_for_single_ingredient(
                db,
                ing_key_name,
                min_linked_similarity_score,
                min_initial_name_similarity
            )
            ingredient_details_cache[ing_key_name] = details

    logger.debug("Aggregating details for each recipe using cached ingredient info.")
    enriched_recipes_list = []
    for recipe in recipes:
        current_recipe_enriched = recipe.copy()
        recipe_parsed_ingredients = current_recipe_enriched.get("parsed_ingredients_details", [])

        if recipe_parsed_ingredients and ingredient_details_cache:
            current_recipe_enriched["aggregated_details"] = _aggregate_details_for_recipe(
                ingredient_details_cache,
                recipe_parsed_ingredients
            )
        else:
            current_recipe_enriched["aggregated_details"] = {} 
            
        enriched_recipes_list.append(current_recipe_enriched)
    
    logger.debug("Recipe enrichment process completed.")
    return enriched_recipes_list
