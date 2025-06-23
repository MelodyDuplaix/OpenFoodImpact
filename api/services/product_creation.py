from typing import Optional, Tuple, List, Dict
from sqlalchemy.orm import Session
from fastapi import HTTPException, status
import logging
import time
from api.sql_models import ProductVector, Agribalyse, OpenFoodFacts, GreenpeaceSeason
from processing.utils import normalize_name, vectorize_name

logger = logging.getLogger(__name__)

def normalize_and_validate_name(product_name: str) -> str:
    """
    Normalise et valide le nom du produit.

    Args:
        product_name (str): Le nom du produit à normaliser.

    Raises:
        HTTPException: Si le nom du produit est invalide.

    Returns:
        str: Le nom du produit normalisé.
    """
    normalized_name = normalize_name(product_name)
    if not normalized_name:
        logger.error("[STEP] Normalized product name is empty after processing.")
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Normalized product name cannot be empty after processing.")
    return normalized_name

def select_or_create_product_vector(db_sqla: Session, normalized_name: str, product_data) -> Tuple[ProductVector, str, List[str]]:
    """
    Sélectionne ou crée un ProductVector basé sur le nom normalisé et les données du produit.

    Args:
        db_sqla (Session): La session SQLAlchemy.
        normalized_name (str): Le nom normalisé du produit.
        product_data (_type_): Les données du produit.

    Raises:
        HTTPException: Si le produit existe déjà.

    Returns:
        Tuple[ProductVector, str, List[str]]: Le ProductVector à traiter, la source effective et les messages d'action.
    """
    action_messages = []
    effective_source = None
    pv_to_process = None
    # on vérifie si des ProductVector existent déjà pour ce nom
    existing_pvs = db_sqla.query(ProductVector).filter(ProductVector.name == normalized_name).all()
    if existing_pvs:
        if len(existing_pvs) == 1:
            # Si un seul ProductVector existe, on le sélectionne
            pv_to_process = existing_pvs[0]
            action_messages.append(f"Found existing ProductVector for '{normalized_name}' (ID: {pv_to_process.id}, Source: {pv_to_process.source}).")
        else:
            # Si plusieurs ProductVector existent, on doit choisir lequel utiliser, en fonction des données fournies
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
                # Si on a trouvé un ProductVector correspondant à une des sources fournies, on l'utilise
                pv_to_process = selected_pv
                action_messages.append(f"Multiple ProductVectors found for '{normalized_name}'. Selected existing PV (ID: {pv_to_process.id}, Source: {pv_to_process.source}) based on provided payloads.")
            else:
                # Si aucun ProductVector ne correspond aux sources fournies, on utilise le premier
                pv_to_process = existing_pvs[0]
                action_messages.append(f"Multiple ProductVectors found for '{normalized_name}'. Defaulting to the first one (ID: {pv_to_process.id}, Source: {pv_to_process.source}).")
        effective_source = getattr(pv_to_process, 'source', '') or ''
    else:
        # Si aucun ProductVector n'existe, on en crée un nouveau
        if product_data.agribalyse_payload:
            effective_source = "agribalyse"
        elif product_data.openfoodfacts_payload:
            effective_source = "openfoodfacts"
        elif product_data.greenpeace_payload:
            effective_source = "greenpeace"
        else:
            logger.error("[STEP] No data payload provided for new product.")
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Cannot create a new product without at least one data payload (Agribalyse, OpenFoodFacts, or Greenpeace).")
        name_vector = vectorize_name(normalized_name)
        new_pv_entry = ProductVector(
            name=normalized_name,
            name_vector=name_vector,
            source=effective_source
        )
        db_sqla.add(new_pv_entry)
        pv_to_process = new_pv_entry
        action_messages.append(f"New ProductVector for '{normalized_name}' created with source '{effective_source}'.")
    return pv_to_process, effective_source, action_messages

def process_agribalyse_payload(db_sqla: Session, pv_to_process: ProductVector, product_data, action_messages: List[str]):
    """
    Traite les données de payload Agribalyse et met à jour ou crée l'entrée correspondante.

    Args:
        db_sqla (Session): La session SQLAlchemy.
        pv_to_process (ProductVector): Le ProductVector à traiter.
        product_data (AgribalyseProductData): Les données du produit.
        action_messages (List[str]): Les messages d'action à mettre à jour.
    """
    if product_data.agribalyse_payload:
        ag_payload_data = product_data.agribalyse_payload.dict(exclude_unset=True)
        ag_name = product_data.agribalyse_payload.nom_produit_francais_agb or product_data.name
        ag_code = product_data.agribalyse_payload.code_agb
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

def process_openfoodfacts_payload(db_sqla: Session, pv_to_process: ProductVector, product_data, action_messages: List[str]):
    """
    Traite les données de payload OpenFoodFacts et met à jour ou crée l'entrée correspondante.


    Args:
        db_sqla (Session): La session SQLAlchemy.
        pv_to_process (ProductVector): Le ProductVector à traiter.
        product_data (OpenFoodFactsProductData): Les données du produit.
        action_messages (List[str]): Les messages d'action à mettre à jour.
    """
    if product_data.openfoodfacts_payload:
        off_payload_data = product_data.openfoodfacts_payload.dict(exclude_unset=True)
        off_name = product_data.openfoodfacts_payload.product_name_off or product_data.name
        off_code = product_data.openfoodfacts_payload.code_off
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

def process_greenpeace_payload(db_sqla: Session, pv_to_process: ProductVector, product_data, action_messages: List[str]):
    """
    Traite les données de payload Greenpeace et met à jour ou crée les entrées correspondantes.

    Args:
        db_sqla (Session): La session SQLAlchemy.
        pv_to_process (ProductVector): Le ProductVector à traiter.
        product_data (GreenpeaceProductData): Les données du produit.
        action_messages (List[str]): Les messages d'action à mettre à jour.
    """
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

def commit_and_refresh(db_sqla: Session, pv_to_process: ProductVector):
    """
    Commit les changements dans la base de données et rafraîchit l'objet ProductVector.

    Args:
        db_sqla (Session): La session SQLAlchemy.
        pv_to_process (ProductVector): Le ProductVector à traiter.

    Raises:
        HTTPException: Si une erreur se produit lors de l'engagement des modifications.
    """
    try:
        db_sqla.commit()
        db_sqla.refresh(pv_to_process)
    except Exception as e:
        db_sqla.rollback()
        logger.error(f"Error during product database insertion: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to save product data: {str(e)}")

def update_ingredient_links(product_vector_id: int, normalized_name: str, effective_source: str, find_similar_ingredients, get_psycopg2_connection, create_ingredient_link_table, logger):
    """
    Met à jour les liens entre les ingrédients dans la base de données.

    Args:
        product_vector_id (int): L'ID du ProductVector à traiter.
        normalized_name (str): Le nom normalisé du produit.
        effective_source (str): La source effective du produit.
        find_similar_ingredients: Fonction pour trouver des ingrédients similaires.
        get_psycopg2_connection: Fonction pour obtenir une connexion à la base de données PostgreSQL.
        create_ingredient_link_table: Fonction pour créer la table de liens d'ingrédients si elle n'existe pas.
        logger: Le logger pour enregistrer les informations de débogage.

    """
    import psycopg2
    import time
    step_times = {}
    step_start = time.perf_counter()
    pg_conn = None
    try:
        pg_conn = get_psycopg2_connection()
        if pg_conn:
            create_ingredient_link_table(pg_conn)
            logger.debug(f"[STEP] Updating ingredient links for new product '{normalized_name}' (ID: {product_vector_id}, Source: {effective_source})")
            # on cherche les ingrédients similaires à partir du nom normalisé
            similars_from_new = find_similar_ingredients(normalized_name, effective_source, pg_conn)
            logger.debug(f"[STEP] Found {len(similars_from_new)} similar ingredients from new product '{normalized_name}'")
            with pg_conn.cursor() as cur:
                for other_src, match_data in similars_from_new.items():
                    cur.execute("""
                        INSERT INTO ingredient_link (id_source, source, id_linked, linked_source, score)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id_source, source, id_linked, linked_source) DO UPDATE SET score = EXCLUDED.score;
                    """, (product_vector_id, effective_source, match_data['id'], other_src, match_data['score']))
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
