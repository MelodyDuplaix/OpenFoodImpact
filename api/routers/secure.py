from typing import Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
import psycopg2 # For find_similar_ingredients
import logging
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from api.auth import get_current_user
from api.models import Recipe # Keep for existing routes, consider moving to schemas if it's a Pydantic model
from api.schemas.product import ProductCreate, ProductCreationResponse
from api.services.db_session import get_db
from api.sql_models import ProductVector, Agribalyse, OpenFoodFacts, GreenpeaceSeason
from api.services.product_creation import (
    normalize_and_validate_name,
    select_or_create_product_vector,
    process_agribalyse_payload,
    process_openfoodfacts_payload,
    process_greenpeace_payload,
    commit_and_refresh,
    update_ingredient_links
)

from processing.utils import normalize_name, vectorize_name, get_db_connection as get_psycopg2_connection
from processing.ingredient_similarity import find_similar_ingredients
from processing.build_ingredient_links import create_ingredient_link_table


import logging
logger = logging.getLogger(__name__)
level = getattr(logging, "DEBUG", None)
logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(module)s %(message)s')


router = APIRouter()

@router.get("/", response_model=Dict[str, str], tags=["User"])
async def get_testroute(user: dict = Depends(get_current_user)):
    """
    Secured test route to check user authentication.  
    
    Args:  
        user (dict): Authenticated user info, injected by Depends(get_user).  
    Returns:  
        dict: Authenticated user info.  
    """
    return user

@router.post("/recipe", response_model=Recipe, deprecated=True, summary="Placeholder for recipe creation", tags=["Updates"])
async def create_recipe(recipe_data: Recipe):
    """
    Create a new recipe (placeholder).  
    
    Args:  
        recipe_data (Recipe): Recipe data to create.  
    Returns:  
        Recipe: The created recipe (currently not implemented).  
    """
    pass

@router.post("/product", response_model=ProductCreationResponse, tags=["Updates"])
async def create_product_endpoint(
    product_data: ProductCreate,
    db_sqla: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Create or update a product (ProductVector) and its associated data (Agribalyse, OpenFoodFacts, Greenpeace) in a single request.  
    Also updates ingredient similarity links.  
    
    Args:  
        product_data (ProductCreate): The product data to create.  
        db_sqla (Session): SQLAlchemy session dependency.  
        current_user (dict): Authenticated user info.  
    
    Returns:  
        ProductCreationResponse: Information about the created product.  
    
    Raises:  
        HTTPException: If the product already exists or on server error.  
    """
    action_messages = []
    step_times = {}
    start_time = time.perf_counter()
    logger.debug("[STEP] Start create_product_endpoint")

    logger.debug("[DEBUG] Step 1: Normalisation et validation du nom")
    normalized_name = normalize_and_validate_name(product_data.name)
    step_times['normalize_name'] = time.perf_counter() - start_time
    logger.debug(f"[STEP] normalize_name done in {step_times['normalize_name']:.4f}s")

    logger.debug("[DEBUG] Step 2: Sélection ou création du ProductVector")
    step_start = time.perf_counter()
    pv_to_process, effective_source, effective_code_source, action_messages_pv = select_or_create_product_vector(db_sqla, normalized_name, product_data)
    action_messages.extend(action_messages_pv)
    step_times['select_or_create_pv'] = time.perf_counter() - step_start
    logger.debug(f"[STEP] select_or_create_pv done in {step_times['select_or_create_pv']:.4f}s")

    try:
        logger.debug("[DEBUG] Step 3: Flush DB session")
        step_start = time.perf_counter()
        db_sqla.flush()
        step_times['flush'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] flush done in {step_times['flush']:.4f}s")

        logger.debug("[DEBUG] Step 4: Traitement Agribalyse")
        step_start = time.perf_counter()
        process_agribalyse_payload(db_sqla, pv_to_process, product_data, action_messages)
        step_times['agribalyse'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] agribalyse done in {step_times['agribalyse']:.4f}s")

        logger.debug("[DEBUG] Step 5: Traitement OpenFoodFacts")
        step_start = time.perf_counter()
        process_openfoodfacts_payload(db_sqla, pv_to_process, product_data, action_messages)
        step_times['openfoodfacts'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] openfoodfacts done in {step_times['openfoodfacts']:.4f}s")

        logger.debug("[DEBUG] Step 6: Traitement Greenpeace")
        step_start = time.perf_counter()
        process_greenpeace_payload(db_sqla, pv_to_process, product_data, action_messages)
        step_times['greenpeace'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] greenpeace done in {step_times['greenpeace']:.4f}s")

        logger.debug("[DEBUG] Step 7: Commit et refresh DB")
        step_start = time.perf_counter()
        commit_and_refresh(db_sqla, pv_to_process)
        step_times['commit_refresh'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] commit_refresh done in {step_times['commit_refresh']:.4f}s")
    except Exception as e:
        db_sqla.rollback()
        logger.error(f"Error during product database insertion: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to save product data: {str(e)}")

    product_vector_id = pv_to_process.id
    if not isinstance(product_vector_id, int) or product_vector_id is None:
        logger.error("Invalid product_vector_id: must be a non-None integer")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Invalid product_vector_id generated.")

    logger.debug("[DEBUG] Step 8: Mise à jour des liens d'ingrédients")
    update_ingredient_links(
        product_vector_id,
        normalized_name,
        effective_source,
        find_similar_ingredients,
        get_psycopg2_connection,
        create_ingredient_link_table,
        logger
    )

    total_time = time.perf_counter() - start_time
    logger.debug(f"[STEP] create_product_endpoint finished in {total_time:.4f}s. Step breakdown: {step_times}")

    return ProductCreationResponse(
        product_vector_id=product_vector_id, # type: ignore
        name=product_data.name,
        normalized_name=normalized_name,
        source=effective_source,
        code_source=effective_code_source,
        message="Product data processed. Details: " + " | ".join(action_messages) + ". Ingredient similarity links update initiated."
    )
