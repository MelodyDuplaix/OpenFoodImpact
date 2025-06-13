from fastapi import APIRouter, Query
from typing import List, Optional
import os
from dotenv import load_dotenv
from processing.utils import normalize_name
import re
from api.db import get_mongodb_connection
from enum import Enum

load_dotenv()

router = APIRouter()

class SortCriteria(str, Enum):
    TOTAL_TIME = "total_time"
    SCORE = "score"

@router.get("/recipes")
async def get_recipes(
    text_search: Optional[str] = Query(None, description="Text to search in title, name, keywords, and description"),
    ingredients: Optional[List[str]] = Query(None, description="List of ingredients to search for"),
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
        query = {}
        
        if total_time_max is not None:
            query["totalTime"] = {"$lte": total_time_max}
        
        if category:
            query["category"] = {"$regex": f".*{category}.*", "$options": "i"}
        
        if ingredients:
            normalized_ingredients = [normalize_name(ing) for ing in ingredients]
            query["$and"] = [{"$or": [
                {"recipeIngredient": {"$regex": f".*{re.escape(ing)}.*", "$options": "i"}},
                {"normalized_ingredients": ing}
            ]} for ing in normalized_ingredients]
        
        if text_search:
            query["$text"] = {"$search": text_search}
        if sort_by == SortCriteria.TOTAL_TIME:
            sort_criteria = [("totalTime", 1)]
        elif sort_by == SortCriteria.SCORE and text_search:
            sort_criteria = [("score", {"$meta": "textScore"})]
        else:
            sort_criteria = None
        if text_search:
            cursor = collection.find(query, {"score": {"$meta": "textScore"}})
        else:
            cursor = collection.find(query)
        
        if sort_criteria:
            cursor = cursor.sort(sort_criteria)
        
        recipes = list(cursor.skip(skip).limit(limit))
        for recipe in recipes:
            if "_id" in recipe:
                del recipe["_id"]
        
        return recipes
    except Exception as e:
        return {"error": f"Error retrieving recipes: {str(e)}"}
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