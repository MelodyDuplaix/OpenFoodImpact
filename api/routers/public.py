from fastapi import APIRouter, Query
from typing import List, Optional
import os
from dotenv import load_dotenv
from processing.utils import normalize_name
import re
from api.db import get_mongodb_connection
from enum import Enum
from api.services.query_helper import build_recipe_query_conditions, get_recipe_sort_criteria, IngredientMatchType, SortCriteria
load_dotenv()

router = APIRouter()



@router.get("/recipes")
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
    Get recipes with optional filtering parameters.
    
    Args:
        text_search (str, optional): Text to search in title, name, keywords, and description
        ingredients (List[str], optional): List of ingredients to search for
        ingredient_match_type (IngredientMatchType, optional): 'all' to match all ingredients, 'any' to match at least one. Defaults to 'all'.
        excluded_ingredients (List[str], optional): List of ingredients to exclude
        category (str, optional): Recipe category
        total_time_max (int, optional): Maximum total time in minutes
        sort_by (SortCriteria): Sorting criteria (total_time or score)
        limit (int, optional): Number of recipes to return (default: 20)
        skip (int, optional): Number of recipes to skip (default: 0)
        
    Returns:
        List[dict]: List of recipes matching the given criteria.
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
            "message":  "Recipes retrieved successfully",
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

@router.get("/products")
async def get_products():
    pass

@router.get("/product/{product_id}")
async def get_product(product_id: int):
    pass