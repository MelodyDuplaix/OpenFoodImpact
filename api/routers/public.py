from fastapi import APIRouter, Query
from typing import List, Optional
import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from processing.utils import normalize_name, vectorize_name
from api.db import get_mongodb_connection
from api.services.query_helper import build_recipe_query_conditions, get_recipe_sort_criteria, IngredientMatchType, SortCriteria # Pour get_recipes
from api.services.product_query_helper import _get_product_vector_ids_by_name, _get_linked_product_vector_ids, _fetch_product_details, _calculate_similarity_to_search_term, _fetch_recipes_for_ingredient, get_pg_connection, get_mongo_client_connection

load_dotenv()

router = APIRouter()

@router.get(
    "/recipes",
    summary="Retrieve a list of recipes",
    description="Get recipes with optional filtering and sorting parameters.",
    response_description="A list of recipes matching the given criteria.",
    responses={
        200: {
            "description": "Successfully retrieved recipes.",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "message": "Recipes retrieved successfully",
                        "data": [
                            {
                                "title": "Recipe Title",
                                "link": "https://example.com/recipe",
                                "category": "entree",
                                "totalTime": "45 minutes",
                                "recipeIngredient": ["ingredient1", "ingredient2"],
                                "recipeInstructions": ["instruction1", "instruction2"],
                                "score": 1.0
                            }
                        ],
                        "count": 1
                    }
                }
            }
        },
        400: {"description": "Invalid request parameters."},
        500: {"description": "Failed to connect to MongoDB or other server error."}
    }
)
async def get_recipes(
    text_search: Optional[str] = Query(None, description="Text to search in title, name, keywords, and description"),
    ingredients: Optional[List[str]] = Query(None, description="List of ingredients to search for"),
    ingredient_match_type: IngredientMatchType = Query(IngredientMatchType.ALL, description="How to match ingredients: 'all' (default) or 'any'"),
    excluded_ingredients: Optional[List[str]] = Query(None, description="List of ingredients to exclude"),
    category: Optional[str] = Query(None, description="Recipe category (entree, plat-principal, dessert, boissons)"),
    total_time_max: Optional[int] = Query(None, description="Maximum total time in minutes"),
    sort_by: SortCriteria = Query(SortCriteria.SCORE, description="Sorting criteria"),
    limit: int = Query(20, description="Number of recipes to return"),
    skip: int = Query(0, description="Number of recipes to skip")
):
    """
    Get recipes with optional filtering and sorting parameters.

    Args:
        text_search (Optional[str]): Text to search in title, name, keywords, and description.
        ingredients (Optional[List[str]]): List of ingredients to search for.
        ingredient_match_type (IngredientMatchType): 'all' to match all ingredients, 'any' to match at least one. Defaults to 'all'.
        excluded_ingredients (Optional[List[str]]): List of ingredients to exclude.
        category (Optional[str]): Recipe category.
        total_time_max (Optional[int]): Maximum total time in minutes.
        sort_by (SortCriteria): Sorting criteria (total_time or score).
        limit (int): Number of recipes to return (default: 20).
        skip (int): Number of recipes to skip (default: 0).

    Returns:
        dict: A dictionary containing the success status, message, data, and count of recipes.
    """
    client = get_mongodb_connection()
    if not client:
        return {"error": "Failed to connect to MongoDB"}
    try:
        db = client["OpenFoodImpact"]
        collection = db["recipes"]

        query_conditions = build_recipe_query_conditions(
            text_search, ingredients, ingredient_match_type,
            excluded_ingredients, category, total_time_max
        )

        mongo_query = {}
        if query_conditions:
            mongo_query["$and"] = query_conditions

        sort_criteria_list = get_recipe_sort_criteria(sort_by, text_search)
        projection = None
        if text_search:
            projection = {"score": {"$meta": "textScore"}}

        cursor = collection.find(mongo_query, projection)

        if sort_criteria_list:
            cursor = cursor.sort(sort_criteria_list)

        recipes = list(cursor.skip(skip).limit(limit))
        for recipe in recipes:
            if "_id" in recipe:
                del recipe["_id"]
        return {
            "success": True,
            "message": "Recipes retrieved successfully",
            "data": recipes,
            "count": len(recipes)
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Error retrieving recipes: {str(e)}",
            "data": [],
            "count": 0
        }
    finally:
        client.close()

@router.get("/recipe/{recipe_id}")
async def get_recipe(recipe_id: int):
    pass

@router.get(
    "/products",
    summary="Retrieve product information and associated recipes",
    description="Get product details from various sources linked by similarity, and associated recipes, based on an ingredient name search.",
    response_description="Product details from all available sources and a list of recipes containing the ingredient."
)
async def get_products(
    name_search: str = Query(..., description="Ingredient name to search for (required)."), # Rendu obligatoire
    min_similarity_score: float = Query(0.65, ge=0, le=1, description="Minimum similarity score for linked products (from ingredient_link table, 0 to 1)"),
    min_name_similarity: float = Query(0.3, ge=0, le=1, description="Minimum fuzzy similarity score for initial name search (pg_trgm similarity, 0 to 1)"),
    limit: int = Query(20, ge=1, description="Number of products to return"),
    skip: int = Query(0, ge=0, description="Number of products to skip")
):
    """
    Get product information linked across sources and associated recipes for a given ingredient name.
    """
    pg_conn = None
    mongo_client = None
    try:
        pg_conn = get_pg_connection()
        mongo_client = get_mongo_client_connection()

        if not pg_conn:
            return {"success": False, "message": "Failed to connect to PostgreSQL", "data": None, "count": 0}

        normalized_search_name = normalize_name(name_search)
        search_vector = vectorize_name(normalized_search_name) # Nécessaire pour _calculate_similarity_to_search_term

        initial_pv_ids = _get_product_vector_ids_by_name(pg_conn, normalized_search_name, min_name_similarity)

        if not initial_pv_ids:
            return {"success": True, "message": "No initial product found for the given name.", "data": {"products": [], "recipes": []}, "count": 0, "recipe_count": 0}

        # Récupérer les meilleurs liens pour chaque produit initial trouvé
        # best_links_map est un dict: {initial_pv_id: {linked_source: {id: ..., name: ..., score: ...}}}
        best_links_map = _get_linked_product_vector_ids(pg_conn, initial_pv_ids, min_similarity_score)

        # Collecter tous les IDs uniques (initiaux + meilleurs liens) pour récupérer leurs détails
        all_unique_pv_ids_to_fetch = set(initial_pv_ids)
        for initial_id in best_links_map:
            for linked_source_data in best_links_map[initial_id].values():
                all_unique_pv_ids_to_fetch.add(linked_source_data['id'])

        if not all_unique_pv_ids_to_fetch:
             products_with_details = [] # Devrait être couvert par le check initial_pv_ids, mais par sécurité
        else:
            products_with_details = _fetch_product_details(pg_conn, all_unique_pv_ids_to_fetch)

        # Calculer le score de similarité par rapport au terme de recherche pour chaque produit
        for product in products_with_details:
            product['score_to_search'] = _calculate_similarity_to_search_term(
                pg_conn, product['id'], normalized_search_name, search_vector
            )
        
        # Filtrer pour ne garder que le meilleur produit par source, basé sur score_to_search
        best_product_per_source: Dict[str, Any] = {}
        # Trier d'abord par score pour faciliter la sélection du meilleur
        products_with_details.sort(key=lambda x: (x.get('source'), x.get('score_to_search', 0.0)), reverse=True)

        for product in products_with_details:
            source = product['source']
            current_score = product.get('score_to_search', 0.0)
            if source not in best_product_per_source or current_score > best_product_per_source[source].get('score_to_search', 0.0):
                best_product_per_source[source] = product
        
        # La liste finale des produits uniques par source, les plus pertinents
        final_products_list = list(best_product_per_source.values())
        # Trier la liste finale globalement par score_to_search
        final_products_list.sort(key=lambda x: x.get('score_to_search', 0.0), reverse=True)

        # Récupérer les recettes associées
        associated_recipes = []
        if mongo_client:
            associated_recipes = _fetch_recipes_for_ingredient(mongo_client, normalized_search_name, limit=10, skip=0) # Pagination simple pour les recettes

        # Construire l'objet 'details' global en fusionnant les informations de tous les produits finaux
        # On ne prend que les champs qui ne sont pas déjà dans product_vector (id, name, source, code_source, score_to_search)
        # et qui ne sont pas des listes/objets complexes pour cet objet 'details' simplifié.
        # Les champs spécifiques à chaque source (identifiants primaires, noms sources) sont exclus ici
        # car ils sont déjà représentés dans la liste "products".
        global_details_aggregator = {}
        # Définir les clés à exclure de global_details_aggregator.
        # Celles-ci sont soit des champs de base de product_vector (déjà dans la liste 'products'),
        # soit des noms/codes spécifiques à la source qui sont aussi représentés dans la liste 'products'.
        excluded_keys_for_global_details = [
            'id', 'name', 'source', 'code_source', 'score_to_search', 'name_vector', # Champs de base ou calculés
            'product_name', 'code', # Champs spécifiques OpenFoodFacts (code est souvent redondant avec code_source)
            'nom_produit_francais', 'code_agb', 'code_ciqual', 'lci_name' # Champs spécifiques Agribalyse
        ]

        for product in final_products_list:
            for key, value in product.items():
                if key not in excluded_keys_for_global_details and value is not None:
                    # Gérer les conflits simples en préfixant par la source si la clé existe déjà et a une valeur différente
                    if key in global_details_aggregator and global_details_aggregator[key] != value:
                        global_details_aggregator[f"{product['source']}_{key}"] = value
                    elif key not in global_details_aggregator:
                        global_details_aggregator[key] = value

        total_product_count = len(final_products_list)
        
        # Créer la liste paginée de produits avec uniquement les champs de base
        products_for_response = []
        for product_detail in final_products_list[skip : skip + limit]:
            products_for_response.append({
                "id": product_detail.get("id"),
                "name": product_detail.get("name"),
                "source": product_detail.get("source"),
                "code_source": product_detail.get("code_source"),
                "score_to_search": product_detail.get("score_to_search")
            })

        return {
            "success": True,
            "message": "Product information and associated recipes retrieved successfully.",
            "data": {"details": global_details_aggregator, "products": products_for_response, "recipes": associated_recipes},
            "count": total_product_count, # Nombre total de produits uniques par source avant pagination
            "recipe_count": len(associated_recipes)
        }
    except Exception as e:
        print(f"Error in /products endpoint: {str(e)}") # Log l'erreur côté serveur
        return {"success": False, "message": f"An error occurred: {str(e)}", "data": None, "count": 0}
    finally:
        if pg_conn:
            pg_conn.close()
        if mongo_client:
            mongo_client.close()

@router.get("/product/{product_id}")
async def get_product(product_id: int):
    pass