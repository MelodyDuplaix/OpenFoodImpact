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

from processing.utils import normalize_name, vectorize_name, get_db_connection as get_psycopg2_connection
from processing.ingredient_similarity import find_similar_ingredients
from processing.build_ingredient_links import create_ingredient_link_table

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/", response_model=Dict[str, str], tags=["User"])
async def get_testroute(user: dict = Depends(get_current_user)):
    """
    Route de test sécurisée pour vérifier l'authentification de l'utilisateur.  

    Args:  
        user (dict): Informations de l'utilisateur authentifié, injectées par Depends(get_user).  
    Returns:  
        dict: Informations de l'utilisateur authentifié.  
    """
    return user

@router.post("/recipe", response_model=Recipe, deprecated=True, summary="Placeholder for recipe creation", tags=["Updates"])
async def create_recipe(recipe_data: Recipe):
    """
    Crée une nouvelle recette (placeholder).  

    Args:  
        recipe_data (Recipe): Données de la recette à créer.  
    Returns:  
        Recipe: La recette créée (actuellement non implémenté).  
    """
    pass

@router.post("/product", response_model=ProductCreationResponse, tags=["Updates"])
async def create_product_endpoint(
    product_data: ProductCreate,
    db_sqla: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Creates or updates a product's main entry (ProductVector) and its associated data  
    across different sources (Agribalyse, OpenFoodFacts, Greenpeace) in a single request.  
    It also updates ingredient similarity links.  
    
    Args:
        product_data (ProductCreate): The product data to create.  
        db_sqla (Session): SQLAlchemy session dependency.  
        current_user (dict): Authenticated user information.  

    Returns:  
        ProductCreationResponse: Information about the created product.  

    Raises:  
        HTTPException: If the product already exists or if there's a server error.  
    """
    action_messages = []
    step_times = {}
    start_time = time.perf_counter()
    logger.debug("[STEP] Start create_product_endpoint")

    normalized_name = normalize_name(product_data.name)
    step_times['normalize_name'] = time.perf_counter() - start_time
    logger.debug(f"[STEP] normalize_name done in {step_times['normalize_name']:.4f}s")
    if not normalized_name:
        logger.error("[STEP] Normalized product name is empty after processing.")
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Normalized product name cannot be empty after processing.")

    pv_to_process = None
    effective_source = None
    effective_code_source = None

    step_start = time.perf_counter()
    existing_pvs = db_sqla.query(ProductVector).filter(ProductVector.name == normalized_name).all()
    step_times['query_existing_pvs'] = time.perf_counter() - step_start
    logger.debug(f"[STEP] query_existing_pvs done in {step_times['query_existing_pvs']:.4f}s")

    step_start = time.perf_counter()
    if existing_pvs:
        if len(existing_pvs) == 1:
            pv_to_process = existing_pvs[0]
            action_messages.append(f"Found existing ProductVector for '{normalized_name}' (ID: {pv_to_process.id}, Source: {pv_to_process.source}).")
        else:
            selected_pv = None
            if product_data.agribalyse_payload:
                for pv in existing_pvs:
                    if getattr(pv, 'source', None) == "agribalyse":
                        selected_pv = pv
                        break
            if not selected_pv and product_data.openfoodfacts_payload:
                for pv in existing_pvs:
                    if getattr(pv, 'source', None) == "openfoodfacts":
                        selected_pv = pv
                        break
            if not selected_pv and product_data.greenpeace_payload:
                for pv in existing_pvs:
                    if getattr(pv, 'source', None) == "greenpeace":
                        selected_pv = pv
                        break
            if selected_pv:
                pv_to_process = selected_pv
                action_messages.append(f"Multiple ProductVectors found for '{normalized_name}'. Selected existing PV (ID: {pv_to_process.id}, Source: {pv_to_process.source}) based on provided payloads.")
            else:
                pv_to_process = existing_pvs[0]
                action_messages.append(f"Multiple ProductVectors found for '{normalized_name}'. Defaulting to the first one (ID: {pv_to_process.id}, Source: {pv_to_process.source}).")

        effective_source = getattr(pv_to_process, 'source', '') or ''
        effective_code_source = getattr(pv_to_process, 'code_source', None)

        pv_instance_source = getattr(pv_to_process, 'source', '') or ''
        pv_instance_code_source = getattr(pv_to_process, 'code_source', None)

        if pv_instance_source == "agribalyse" and \
           product_data.agribalyse_payload and \
           getattr(product_data.agribalyse_payload, 'code_agb', None) is not None and \
           pv_instance_code_source != getattr(product_data.agribalyse_payload, 'code_agb', None):
            setattr(pv_to_process, 'code_source', product_data.agribalyse_payload.code_agb)
            action_messages.append(f"ProductVector's primary code_source updated to '{product_data.agribalyse_payload.code_agb}' based on Agribalyse payload.")
        elif pv_instance_source == "openfoodfacts" and \
             product_data.openfoodfacts_payload and \
             getattr(product_data.openfoodfacts_payload, 'code_off', None) is not None and \
             pv_instance_code_source != getattr(product_data.openfoodfacts_payload, 'code_off', None):
            setattr(pv_to_process, 'code_source', product_data.openfoodfacts_payload.code_off)
            action_messages.append(f"ProductVector's primary code_source updated to '{product_data.openfoodfacts_payload.code_off}' based on OpenFoodFacts payload.")
        effective_code_source = getattr(pv_to_process, 'code_source', None)
    else: # No existing ProductVector found, create a new one
        if product_data.agribalyse_payload:
            effective_source = "agribalyse"
            effective_code_source = product_data.agribalyse_payload.code_agb
        elif product_data.openfoodfacts_payload:
            effective_source = "openfoodfacts"
            effective_code_source = product_data.openfoodfacts_payload.code_off
        elif product_data.greenpeace_payload:
            effective_source = "greenpeace"
            effective_code_source = None # Greenpeace doesn't have a 'code_source' in the same way
        else:
            logger.error("[STEP] No data payload provided for new product.")
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Cannot create a new product without at least one data payload (Agribalyse, OpenFoodFacts, or Greenpeace).")

        name_vector = vectorize_name(normalized_name)
        new_pv_entry = ProductVector(
            name=normalized_name,
            name_vector=name_vector,
            source=effective_source,
            code_source=effective_code_source
        )
        db_sqla.add(new_pv_entry)
        pv_to_process = new_pv_entry
        action_messages.append(f"New ProductVector for '{normalized_name}' created with source '{effective_source}' and code '{effective_code_source}'.")
    step_times['select_or_create_pv'] = time.perf_counter() - step_start
    logger.debug(f"[STEP] select_or_create_pv done in {step_times['select_or_create_pv']:.4f}s")

    try:
        step_start = time.perf_counter()
        db_sqla.flush()
        step_times['flush'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] flush done in {step_times['flush']:.4f}s")

        # Process Agribalyse Payload
        step_start = time.perf_counter()
        if product_data.agribalyse_payload:
            ag_payload_data = product_data.agribalyse_payload.dict(exclude_unset=True)
            ag_name = product_data.agribalyse_payload.nom_produit_francais_agb or product_data.name
            ag_code = product_data.agribalyse_payload.code_agb
            if ag_code is None and effective_source == "agribalyse":
                ag_code = effective_code_source
            ag_entry = db_sqla.query(Agribalyse).filter_by(product_vector_id=pv_to_process.id).first()
            if ag_entry:
                setattr(ag_entry, 'nom_produit_francais', ag_name)
                setattr(ag_entry, 'code_agb', ag_code)
                for key, value in ag_payload_data.items():
                    if hasattr(ag_entry, key) and key not in ["nom_produit_francais_agb", "code_agb"]:
                        setattr(ag_entry, key, value)
                action_messages.append("Agribalyse data updated.")
            else:
                new_ag_entry = Agribalyse(
                    product_vector_id=pv_to_process.id,
                    nom_produit_francais=ag_name,
                    code_agb=ag_code,
                    **{k: v for k, v in ag_payload_data.items() if k not in ["nom_produit_francais_agb", "code_agb"]}
                )
                db_sqla.add(new_ag_entry)
                action_messages.append("Agribalyse data created.")
        step_times['agribalyse'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] agribalyse done in {step_times['agribalyse']:.4f}s")

        # Process OpenFoodFacts Payload
        step_start = time.perf_counter()
        if product_data.openfoodfacts_payload:
            off_payload_data = product_data.openfoodfacts_payload.dict(exclude_unset=True)
            off_name = product_data.openfoodfacts_payload.product_name_off or product_data.name
            off_code = product_data.openfoodfacts_payload.code_off
            if off_code is None and effective_source == "openfoodfacts":
                off_code = effective_code_source
            off_entry = db_sqla.query(OpenFoodFacts).filter_by(product_vector_id=pv_to_process.id).first()
            if off_entry:
                setattr(off_entry, 'product_name', off_name)
                setattr(off_entry, 'code', off_code)
                for key, value in off_payload_data.items():
                    if hasattr(off_entry, key) and key not in ["product_name_off", "code_off"]:
                        setattr(off_entry, key, value)
                action_messages.append("OpenFoodFacts data updated.")
            else:
                new_off_entry = OpenFoodFacts(
                    product_vector_id=pv_to_process.id,
                    product_name=off_name,
                    code=off_code,
                    **{k: v for k, v in off_payload_data.items() if k not in ["product_name_off", "code_off"]}
                )
                db_sqla.add(new_off_entry)
                action_messages.append("OpenFoodFacts data created.")
        step_times['openfoodfacts'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] openfoodfacts done in {step_times['openfoodfacts']:.4f}s")

        # Process Greenpeace Payload
        step_start = time.perf_counter()
        if product_data.greenpeace_payload:
            existing_gp_entries = db_sqla.query(GreenpeaceSeason).filter_by(product_vector_id=pv_to_process.id).all()
            for entry in existing_gp_entries:
                db_sqla.delete(entry)
            if product_data.greenpeace_payload.months:
                for month in product_data.greenpeace_payload.months:
                    new_gp_entry = GreenpeaceSeason(
                        product_vector_id=pv_to_process.id,
                        month=month
                    )
                    db_sqla.add(new_gp_entry)
                action_messages.append("Greenpeace seasonality data updated.")
            else:
                action_messages.append("Greenpeace seasonality data cleared (empty month list provided).")
        step_times['greenpeace'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] greenpeace done in {step_times['greenpeace']:.4f}s")

        step_start = time.perf_counter()
        db_sqla.commit()
        db_sqla.refresh(pv_to_process)
        step_times['commit_refresh'] = time.perf_counter() - step_start
        logger.debug(f"[STEP] commit_refresh done in {step_times['commit_refresh']:.4f}s")
    except Exception as e:
        db_sqla.rollback()
        logger.error(f"Error during product database insertion: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to save product data: {str(e)}")

    product_vector_id = pv_to_process.id
    

    # Update ingredient links
    step_start = time.perf_counter()
    pg_conn = None
    try:
        pg_conn = get_psycopg2_connection()
        if pg_conn:
            create_ingredient_link_table(pg_conn)
            # Liens FROM new product TO others
            logger.debug(f"[STEP] Updating ingredient links for new product '{normalized_name}' (ID: {product_vector_id}, Source: {effective_source})")
            similars_from_new = find_similar_ingredients(normalized_name, effective_source, pg_conn)
            logger.debug(f"[STEP] Found {len(similars_from_new)} similar ingredients from new product '{normalized_name}'")
            with pg_conn.cursor() as cur:
                for other_src, match_data in similars_from_new.items():
                    cur.execute("""
                        INSERT INTO ingredient_link (id_source, source, id_linked, linked_source, score)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id_source, source, id_linked, linked_source) DO UPDATE SET score = EXCLUDED.score;
                    """, (product_vector_id, effective_source, match_data['id'], other_src, match_data['score']))
                # Liens TO new product FROM others
                logger.debug(f"[STEP] Updating ingredient links for existing products matching '{normalized_name}'")
                cur.execute("SELECT id, name, source FROM product_vector WHERE id != %s", (product_vector_id,))
                for ex_id, ex_name, ex_source in cur.fetchall():
                    similars_from_existing = find_similar_ingredients(ex_name, ex_source, pg_conn)
                    if effective_source in similars_from_existing and similars_from_existing[effective_source]['id'] == product_vector_id:
                        match_to_new = similars_from_existing[effective_source]
                        cur.execute("""
                            INSERT INTO ingredient_link (id_source, source, id_linked, linked_source, score)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (id_source, source, id_linked, linked_source) DO UPDATE SET score = EXCLUDED.score;
                        """, (ex_id, ex_source, product_vector_id, effective_source, match_to_new['score']))
            pg_conn.commit()
        else:
            logger.error("Failed to get psycopg2 connection for ingredient link update.")
    except Exception as e_links:
        logger.error(f"Error during ingredient link update: {e_links}", exc_info=True)
    finally:
        if pg_conn:
            pg_conn.close()
    step_times['ingredient_links'] = time.perf_counter() - step_start
    logger.debug(f"[STEP] ingredient_links done in {step_times['ingredient_links']:.4f}s")

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
