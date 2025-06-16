from fastapi import APIRouter, Query
from typing import List, Optional
import os
from dotenv import load_dotenv
from processing.utils import normalize_name
import re
from api.db import get_mongodb_connection
from enum import Enum

load_dotenv()

class SortCriteria(str, Enum):
    TOTAL_TIME = "total_time"
    SCORE = "score"

class IngredientMatchType(str, Enum):
    ALL = "all"  # La recette doit contenir TOUS les ingrédients spécifiés
    ANY = "any"  # La recette doit contenir AU MOINS UN des ingrédients spécifiés


def build_recipe_query_conditions(
    text_search: Optional[str],
    ingredients: Optional[List[str]],
    ingredient_match_type: IngredientMatchType,
    excluded_ingredients: Optional[List[str]],
    category: Optional[str],
    total_time_max: Optional[int]
) -> List[dict]:
    """
    Construit la liste des conditions de filtre pour la requête de recettes MongoDB.

    Args:
        text_search: Texte pour la recherche full-text.
        ingredients: Liste des ingrédients à inclure.
        ingredient_match_type: Mode de correspondance pour les ingrédients (ALL ou ANY).
        excluded_ingredients: Liste des ingrédients à exclure.
        category: Catégorie de recette.
        total_time_max: Temps total maximum de préparation.

    Returns:
        Liste des conditions de requête MongoDB.
    """
    all_conditions = []

    if total_time_max is not None:
        all_conditions.append({"totalTime": {"$lte": total_time_max}})

    if category:
        all_conditions.append({"category": {"$regex": f".*{category}.*", "$options": "i"}})

    if ingredients:
        normalized_ingredients_list = [normalize_name(ing) for ing in ingredients]
        individual_ingredient_conditions = []
        for ing_raw, ing_norm in zip(ingredients, normalized_ingredients_list):
            individual_ingredient_conditions.append({"$or": [
                {"recipeIngredient": {"$regex": f".*{re.escape(ing_raw)}.*", "$options": "i"}},
                {"normalized_ingredients": ing_norm}
            ]})
        
        if ingredient_match_type == IngredientMatchType.ALL:
            all_conditions.extend(individual_ingredient_conditions)
        elif ingredient_match_type == IngredientMatchType.ANY and individual_ingredient_conditions:
            all_conditions.append({"$or": individual_ingredient_conditions})

    if excluded_ingredients:
        normalized_excluded_ingredients = [normalize_name(ex_ing) for ex_ing in excluded_ingredients]
        for ex_ing_raw, ex_ing_norm in zip(excluded_ingredients, normalized_excluded_ingredients):
            all_conditions.append({
                "$nor": [
                    {"recipeIngredient": {"$regex": f".*{re.escape(ex_ing_raw)}.*", "$options": "i"}},
                    {"normalized_ingredients": ex_ing_norm}
                ]
            })

    if text_search:
        all_conditions.append({"$text": {"$search": text_search}})
    
    return all_conditions

def get_recipe_sort_criteria(sort_by: SortCriteria, text_search: Optional[str]) -> Optional[List[tuple]]:
    """Détermine les critères de tri pour la requête MongoDB."""
    if sort_by == SortCriteria.TOTAL_TIME:
        return [("totalTime", 1)]
    elif sort_by == SortCriteria.SCORE and text_search: # textScore ne fonctionne que si $text est dans la requête
        return [("score", {"$meta": "textScore"})]
    return None