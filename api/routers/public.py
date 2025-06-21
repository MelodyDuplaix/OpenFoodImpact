import logging
from fastapi import APIRouter, Query, Path, HTTPException, status, Depends
from typing import Any, Dict, List, Optional # type: ignore
import os
from dotenv import load_dotenv
import sys
from bson import ObjectId
from sqlalchemy.orm import Session

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from processing.utils import normalize_name, vectorize_name
from api.db import get_mongodb_connection
from api.services.db_session import get_db
from api.services.query_helper import build_recipe_query_conditions, get_recipe_sort_criteria, IngredientMatchType, SortCriteria
from api.services.product_query_helper import _get_linked_product_vector_ids, _get_product_vector_ids_by_name, _fetch_recipes_for_ingredient, _get_processed_products, _aggregate_product_details, get_enriched_recipes_details

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
    min_initial_name_similarity_for_details: float = Query(0.25, ge=0, le=1, description="When including details: minimum fuzzy similarity for initial ingredient name search (0-1)."),
    db_pg: Session = Depends(get_db)
):
    """
    Retrieve a list of recipes with optional filters and sorting.  
    
    Args:  
        text_search: Text for full-text search.  
        ingredients: List of ingredients to include.  
        ingredient_match_type: Ingredient match mode ('all' or 'any').  
        excluded_ingredients: List of ingredients to exclude.  
        category: Recipe category.  
        total_time_max: Maximum total time in minutes.  
        sort_by: Sorting criteria ('total_time' or 'score').  
        limit: Number of recipes to return.  
        skip: Number of recipes to skip.  
        include_details: Include aggregated nutritional and environmental details.  
        min_linked_similarity_score_for_details: Minimum similarity score for linked products (if include_details=True).  
        min_initial_name_similarity_for_details: Minimum similarity for initial ingredient name search (if include_details=True).  
    Returns:  
        dict: Dictionary with status, message, recipe data, and total count.  
    """
    mongo_client = get_mongodb_connection()
    if not mongo_client:
        return {"success": False, "message": "Failed to connect to MongoDB", "data": [], "count": 0}
    try:
        db_mongo = mongo_client["OpenFoodImpact"]
        collection = db_mongo["recipes"]

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

        recipes_data = list(cursor.skip(skip).limit(limit))

        if include_details and recipes_data:
            try:
                recipes_data = get_enriched_recipes_details(
                    db_pg,
                    recipes_data,
                    min_linked_similarity_score_for_details,
                    min_initial_name_similarity_for_details
                )
            except Exception as e_pg:
                for r_item in recipes_data: r_item["aggregated_details_error"] = f"Error fetching details: {str(e_pg)}"
            
        for recipe in recipes_data:
            if "_id" in recipe and isinstance(recipe["_id"], ObjectId):
                recipe["_id"] = str(recipe["_id"])
        return {
            "success": True,
            "message": "Recipes retrieved successfully",
            "data": recipes_data,
            "count": total_recipes_count
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Error retrieving recipes: {str(e)}",
            "data": recipes_data,
            "count": total_recipes_count
        }
    finally:
        if mongo_client:
            mongo_client.close()


@router.get(
    "/recipes/{recipe_id}",
    summary="Retrieve a specific recipe by its ID",
    description="Get detailed information for a single recipe, including aggregated nutritional and environmental details for its ingredients if available.",
    response_description="The recipe details.",
    responses={
        200: {
            "description": "Successfully retrieved recipe.",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "message": "Recipe retrieved successfully",
                        "data": {
                            "_id": "60c72b2f9b1e8a3f4c8b4567",
                            "title": "Delicious Pasta",
                            "category": "plat-principal",
                            "recipeIngredient": ["pasta", "tomato sauce", "cheese"],
                            "parsed_ingredients_details": [
                                {
                                    "raw_text": "250g pasta",
                                    "quantity_str": "250",
                                    "unit_str": "g",
                                    "parsed_name": "pasta",
                                    "quantity_grams": 250,
                                    "normalized_name_for_matching": "pasta"
                                }
                            ],
                            "aggregated_details": {
                                "energy_kcal_100g": 350.5,
                                "changement_climatique": 1.2
                            }
                        }
                    }
                }
            }
        },
        400: {"description": "Invalid recipe ID format."},
        404: {"description": "Recipe not found."},
        500: {"description": "Server error."}
    }
)
async def get_recipe_by_id(
    recipe_id: str = Path(..., description="The MongoDB ObjectId of the recipe."),
    min_linked_similarity_score_for_details: float = Query(0.60, ge=0, le=1, description="Minimum similarity score for linked products (0-1) for ingredient details."),
    min_initial_name_similarity_for_details: float = Query(0.25, ge=0, le=1, description="Minimum fuzzy similarity for initial ingredient name search (0-1) for ingredient details."),
    db_pg: Session = Depends(get_db)
):
    """
    Retrieve a specific recipe by its MongoDB ObjectId, with enriched details.  
    
    Args:  
        recipe_id: MongoDB ObjectId of the recipe.  
        min_linked_similarity_score_for_details: Minimum similarity score for linked products during enrichment.  
        min_initial_name_similarity_for_details: Minimum similarity for initial ingredient name search during enrichment.  
    Returns:  
        dict: Dictionary with status, message, and recipe data.  
    Raises:  
        HTTPException: If connection fails, ID is invalid, or recipe not found.  
    """
    mongo_client = get_mongodb_connection()

    if not mongo_client:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to connect to MongoDB")

    try:
        db_mongo = mongo_client["OpenFoodImpact"]
        collection = db_mongo["recipes"]

        try:
            object_id = ObjectId(recipe_id)
        except Exception:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid recipe ID format")

        recipe_data = collection.find_one({"_id": object_id})

        if not recipe_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Recipe not found")

        if "_id" in recipe_data and isinstance(recipe_data["_id"], ObjectId):
            recipe_data["_id"] = str(recipe_data["_id"])
        try:
            enriched_list = get_enriched_recipes_details(
                db_pg, [recipe_data.copy()], min_linked_similarity_score_for_details, min_initial_name_similarity_for_details
            )
            if enriched_list:
                recipe_data = enriched_list[0]
        except Exception as e_pg:
            recipe_data["aggregated_details_error"] = f"Error fetching ingredient details: {str(e_pg)}"
        
        return {"success": True, "message": "Recipe retrieved successfully", "data": recipe_data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")
    finally:
        if mongo_client:
            mongo_client.close()

@router.get(
    "/products",
    summary="Retrieve linked product information and associated recipes for an ingredient name",
    description="Get product and recipe information for a given ingredient name, with similarity thresholds.",
    response_description="Product and recipe information for the given ingredient name.",
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
    skip: int = Query(0, ge=0, description="Number of products to skip"),
    db_pg: Session = Depends(get_db)
):
    """
    Retrieve linked product information and associated recipes for an ingredient name.  
    
    Args:  
        name_search: Ingredient name to search for (required).  
        min_similarity_score: Minimum similarity score for linked products (ingredient_link table).  
        min_name_similarity: Minimum similarity score for initial name search (pg_trgm).  
        limit: Number of products to return.  
        skip: Number of products to skip.  
    Returns:  
        dict: Dictionary with status, message, product and recipe data, and count.  
    """
    mongo_client = None
    print("appel requete")
    try:
        mongo_client = get_mongodb_connection()
        normalized_search_name = normalize_name(name_search)
        search_vector = vectorize_name(normalized_search_name)
        
        print("recherche ingredients")

        initial_pv_ids = _get_product_vector_ids_by_name(db_pg, normalized_search_name, min_name_similarity)

        if not initial_pv_ids:
            return {"success": True, "message": "No initial product found for the given name.", "data": {"products": [], "recipes": []}, "count": 0, "recipe_count": 0}

        print("recherche liens")

        best_links_map = _get_linked_product_vector_ids(db_pg, initial_pv_ids, min_similarity_score)

        all_unique_pv_ids_to_fetch = set(initial_pv_ids)
        for initial_id in best_links_map:
            for linked_source_data in best_links_map[initial_id].values():
                all_unique_pv_ids_to_fetch.add(linked_source_data['id'])
                
        print("recherche details")
        
        final_products_list = _get_processed_products(
            db_pg,
            all_unique_pv_ids_to_fetch,
            normalized_search_name,
            search_vector
        )

        associated_recipes = []
        print("recherche recettes")
        if mongo_client:
            associated_recipes = _fetch_recipes_for_ingredient(mongo_client, normalized_search_name, limit=10, skip=0)
        
        print("aggregation")
        global_details_aggregator = _aggregate_product_details(final_products_list)

        total_product_count = len(final_products_list)

        products_for_response = []
        for product_detail in final_products_list[skip : skip + limit]:
            products_for_response.append({
                "id": product_detail.get("id"),
                "name": product_detail.get("name"),
                "source": product_detail.get("source"),
                "score_to_search": product_detail.get("score_to_search")
            })

        return {
            "success": True,
            "message": "Product information and associated recipes retrieved successfully.",
            "data": {"details": global_details_aggregator, "products": products_for_response, "recipes": associated_recipes},
            "count": len(associated_recipes)
        }
    except Exception as e:
        return {"success": False, "message": f"An error occurred: {str(e)}", "data": None, "count": 0}
    finally:
        # db_pg géré par FastAPI
        if mongo_client:
            mongo_client.close()
