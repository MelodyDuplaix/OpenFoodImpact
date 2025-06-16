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
        query = {}
        all_conditions = []
        
        if total_time_max is not None:
            all_conditions.append({"totalTime": {"$lte": total_time_max}})
        
        if category:
            all_conditions.append({"category": {"$regex": f".*{category}.*", "$options": "i"}})
        
        if ingredients:
            normalized_ingredients = [normalize_name(ing) for ing in ingredients]
            for ing_raw, ing_norm in zip(ingredients, normalized_ingredients):
                all_conditions.append({"$or": [
                    {"recipeIngredient": {"$regex": f".*{re.escape(ing_raw)}.*", "$options": "i"}},
                    {"normalized_ingredients": ing_norm} # Matches if ing_norm is an element of the array
                ]})
                
        if excluded_ingredients: # Ingredients to EXCLUDE
            normalized_excluded_ingredients = [normalize_name(ex_ing) for ex_ing in excluded_ingredients]
            for ex_ing_raw, ex_ing_norm in zip(excluded_ingredients, normalized_excluded_ingredients):
                # For each excluded ingredient, the recipe must NOT contain it in either field.
                # $nor ensures the document fails both conditions (i.e., is not matched by either).
                all_conditions.append({
                    "$nor": [
                        {"recipeIngredient": {"$regex": f".*{re.escape(ex_ing_raw)}.*", "$options": "i"}},
                        {"normalized_ingredients": ex_ing_norm} # Matches if ex_ing_norm is an element of the array
                    ]
                })

        
        if text_search:
            all_conditions.append({"$text": {"$search": text_search}})

        if all_conditions:
            query["$and"] = all_conditions
            
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