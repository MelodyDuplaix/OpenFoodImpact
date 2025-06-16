from fastapi import APIRouter, Query
from typing import Any, Dict, List, Optional
import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from processing.utils import normalize_name, vectorize_name
from api.db import get_mongodb_connection
from api.services.query_helper import build_recipe_query_conditions, get_recipe_sort_criteria, IngredientMatchType, SortCriteria
from api.services.product_query_helper import _get_product_vector_ids_by_name, _get_linked_product_vector_ids, _fetch_recipes_for_ingredient, get_pg_connection, get_mongo_client_connection, _get_processed_products, _aggregate_product_details, get_enriched_recipes_details

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
    limit: int = Query(20, ge=1, description="Number of recipes to return"),
    skip: int = Query(0, ge=0, description="Number of recipes to skip"),
    include_details: bool = Query(False, description="Include aggregated nutritional and environmental details for each recipe. This can significantly increase response time."),
    min_linked_similarity_score_for_details: float = Query(0.60, ge=0, le=1, description="When including details: minimum similarity score for linked products (0-1)."),
    min_initial_name_similarity_for_details: float = Query(0.25, ge=0, le=1, description="When including details: minimum fuzzy similarity for initial ingredient name search (0-1).")
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
        include_details (bool): If True, attempts to fetch and aggregate nutritional and environmental data for ingredients. Defaults to False.
        min_linked_similarity_score_for_details (float): Used if include_details is True. See /products endpoint for similar parameter.
        min_initial_name_similarity_for_details (float): Used if include_details is True. See /products endpoint for similar parameter.

    Returns:
        dict: A dictionary containing the success status, message, data, and count of recipes.
    """
    client = get_mongodb_connection()
    if not client:
        return {"error": "Failed to connect to MongoDB"}
    try: # MongoDB operations
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

        total_recipes_count = collection.count_documents(mongo_query)

        cursor = collection.find(mongo_query, projection)

        if sort_criteria_list:
            cursor = cursor.sort(sort_criteria_list)

        recipes = list(cursor.skip(skip).limit(limit))
        
        for recipe in recipes:
            if "_id" in recipe:
                del recipe["_id"]

        if include_details and recipes:
            pg_conn = None
            try: # PostgreSQL operations for enriching details
                pg_conn = get_pg_connection()
                if pg_conn:
                    recipes = get_enriched_recipes_details(
                        pg_conn,
                        recipes,
                        min_linked_similarity_score_for_details,
                        min_initial_name_similarity_for_details
                    )
                else:
                    for r_item in recipes: r_item["aggregated_details_error"] = "Could not connect to PostgreSQL for details."
            except Exception as e_pg:
                print(f"Error enriching recipes with PostgreSQL details: {e_pg}")
                for r_item in recipes: r_item["aggregated_details_error"] = f"Error fetching details: {str(e_pg)}"
            finally:
                if pg_conn:
                    pg_conn.close()
        elif include_details and not recipes:
            # No recipes found, no need to attempt enrichment
            pass

        return {
            "success": True,
            "message": "Recipes retrieved successfully",
            "data": recipes,
            "count": total_recipes_count
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

@router.get(
    "/products",
    summary="Retrieve product information and associated recipes",
    description="Get product details from various sources linked by similarity, and associated recipes, based on an ingredient name search.",
    response_description="Product details from all available sources and a list of recipes containing the ingredient.",
    responses={
        200: {
            "description": "Successfully retrieved product information and associated recipes.",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "message": "Product information and associated recipes retrieved successfully.",
                        "data": {
                            "details": {},
                            "products": [
                                {
                                    "id": 1,
                                    "name": "Product Name",
                                    "source": "source_name",
                                    "code_source": "code",
                                    "score_to_search": 0.85
                                }
                            ],
                            "recipes": [
                                {
                                    "title": "Recipe Title",
                                    "link": "https://example.com/recipe",
                                    "category": "entree",
                                    "totalTime": "45 minutes",
                                    "recipeIngredient": ["ingredient1", "ingredient2"],
                                    "recipeInstructions": ["instruction1", "instruction2"],
                                    "score": 1.0
                                }
                            ]
                        },
                        "count": 1
                    }
                }
            }
        },
        400: {"description": "Invalid request parameters."},
        500: {"description": "Failed to connect to databases or other server error."}
    }
)
async def get_products(
    name_search: str = Query(..., description="Ingredient name to search for (required)."),
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
        search_vector = vectorize_name(normalized_search_name)

        initial_pv_ids = _get_product_vector_ids_by_name(pg_conn, normalized_search_name, min_name_similarity)

        if not initial_pv_ids:
            return {"success": True, "message": "No initial product found for the given name.", "data": {"products": [], "recipes": []}, "count": 0, "recipe_count": 0}

        best_links_map = _get_linked_product_vector_ids(pg_conn, initial_pv_ids, min_similarity_score)

        all_unique_pv_ids_to_fetch = set(initial_pv_ids)
        for initial_id in best_links_map:
            for linked_source_data in best_links_map[initial_id].values():
                all_unique_pv_ids_to_fetch.add(linked_source_data['id'])

        final_products_list = _get_processed_products(
            pg_conn,
            all_unique_pv_ids_to_fetch,
            normalized_search_name,
            search_vector
        )

        associated_recipes = []
        if mongo_client:
            associated_recipes = _fetch_recipes_for_ingredient(mongo_client, normalized_search_name, limit=10, skip=0)
        
        global_details_aggregator = _aggregate_product_details(final_products_list)

        total_product_count = len(final_products_list)

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
            "count": len(associated_recipes)
        }
    except Exception as e:
        print(f"Error in /products endpoint: {str(e)}")
        return {"success": False, "message": f"An error occurred: {str(e)}", "data": None, "count": 0}
    finally:
        if pg_conn:
            pg_conn.close()
        if mongo_client:
            mongo_client.close()
