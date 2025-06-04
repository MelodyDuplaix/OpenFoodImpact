import time
import requests
import os
import sys
import psycopg2
from .utils import get_db_connection, normalize_name, vectorize_name, safe_execute, handle_error
from .column_mappings import AGRIBALYSE_MAPPING

agribalyse_url = "https://data.ademe.fr/data-fair/api/v1/datasets/agribalyse-31-synthese/lines?after=1&page=1&size=24&sort=&select=Code_AGB,Code_CIQUAL,Groupe_d%27aliment,Sous-groupe_d%27aliment,Nom_du_Produit_en_Fran%C3%A7ais,LCI_Name,code_avion,Livraison,Approche_emballage_,Pr%C3%A9paration,Score_unique_EF,Changement_climatique,Appauvrissement_de_la_couche_d%27ozone,Rayonnements_ionisants,Formation_photochimique_d%27ozone,Particules_fines,Effets_toxicologiques_sur_la_sant%C3%A9_humaine___substances_non-canc%C3%A9rog%C3%A8nes,Effets_toxicologiques_sur_la_sant%C3%A9_humaine___substances_canc%C3%A9rog%C3%A8nes,Acidification_terrestre_et_eaux_douces,Eutrophisation_eaux_douces,Eutrophisation_marine,Eutrophisation_terrestre,%C3%89cotoxicit%C3%A9_pour_%C3%A9cosyst%C3%A8mes_aquatiques_d%27eau_douce,Utilisation_du_sol,%C3%89puisement_des_ressources_eau,%C3%89puisement_des_ressources_%C3%A9nerg%C3%A9tiques,%C3%89puisement_des_ressources_min%C3%A9raux,Changement_climatique_-_%C3%A9missions_biog%C3%A9niques,Changement_climatique_-_%C3%A9missions_fossiles,Changement_climatique_-_%C3%A9missions_li%C3%A9es_au_changement_d%27affectation_des_sols&format=json&q_mode=simple"

def extract_agribalyse_data():
    """Extract Agribalyse data from the API.

    Returns:
        list: List of raw records from the API.
    """
    all_data = []
    try:
        response = requests.get(agribalyse_url)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data["results"])
        while data.get("next", None):
            next_url = data["next"]
            response = requests.get(next_url)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data["results"])
        return all_data
    except Exception as e:
        handle_error(e, 'Extraction Agribalyse')
        return []

def transform_agribalyse_record(record):
    """Transform a raw Agribalyse record using the mapping.

    Args:
        record (dict): Raw record from API
    Returns:
        dict: Cleaned and mapped record
    """
    return {AGRIBALYSE_MAPPING.get(k, k): v for k, v in record.items()}

def load_agribalyse_data_to_db(agribalyse_data):
    """Load Agribalyse data into the database using batch and transaction.

    Args:
        agribalyse_data (list): List of cleaned records
    Returns:
        None
    """
    conn = get_db_connection()
    if conn is None:
        handle_error(Exception('Database connection is None'), 'Load Agribalyse')
        return
    cur = conn.cursor()
    try:
        for record in agribalyse_data:
            record_clean = transform_agribalyse_record(record)
            name = record_clean.get('nom_produit_francais')
            if not name:
                continue
            name_normalized = normalize_name(name)
            name_vector = vectorize_name(name_normalized)
            code_agb = record_clean.get('code_agb')
            code_ciqual = record_clean.get('code_ciqual')
            lci_name = record_clean.get('lci_name')
            # Insert into product_vector
            safe_execute(cur, """
                INSERT INTO product_vector (name, name_vector, source, code_source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id;
            """, (name_normalized, name_vector, 'agribalyse', code_agb or code_ciqual))
            result = cur.fetchone()
            if result:
                product_vector_id = result[0]
            else:
                safe_execute(cur, "SELECT id FROM product_vector WHERE name = %s AND source = %s;", (name_normalized, 'agribalyse'))
                fetch = cur.fetchone()
                if not fetch:
                    continue
                product_vector_id = fetch[0]
            # Prepare columns for agribalyse
            agribalyse_cols = [
                'product_vector_id', 'code_agb', 'code_ciqual', 'lci_name', 'nom_produit_francais',
                'changement_climatique', 'score_unique_ef', 'ecotoxicite_eau_douce', 'epuisement_ressources_energetiques',
                'eutrophisation_marine', 'sous_groupe_aliment', 'effets_tox_cancerogenes', 'approche_emballage',
                'epuisement_ressources_eau', 'eutrophisation_terrestre', 'utilisation_sol', 'code_avion',
                'effets_tox_non_cancerogenes', 'epuisement_ressources_mineraux', 'particules_fines',
                'formation_photochimique_ozone', 'livraison', 'preparation', 'changement_climatique_biogenique',
                'acidification_terrestre_eaux_douces', 'groupe_aliment', 'changement_climatique_cas',
                'appauvrissement_couche_ozone', 'rayonnements_ionisants', 'eutrophisation_eaux_douces',
                'changement_climatique_fossile', 'score'
            ]
            values = [product_vector_id] + [record_clean.get(col) for col in agribalyse_cols[1:]]
            placeholders = ', '.join(['%s'] * len(agribalyse_cols))
            insert_sql = f"INSERT INTO agribalyse ({', '.join(agribalyse_cols)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
            safe_execute(cur, insert_sql, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        handle_error(e, 'Load Agribalyse')
    finally:
        cur.close()
        conn.close()

def etl_agribalyse():
    """ETL pipeline for Agribalyse data (extract, transform, load).

    Returns:
        None
    """
    raw_data = extract_agribalyse_data()
    if raw_data:
        load_agribalyse_data_to_db(raw_data)

def get_agribalyse_data():
    """Alias for extract_agribalyse_data for backward compatibility.

    Returns:
        list: List of raw records from the API.
    """
    return extract_agribalyse_data()

def insert_agribalyse_data_to_db(agribalyse_data):
    """Alias for load_agribalyse_data_to_db for backward compatibility.

    Args:
        agribalyse_data (list): List of cleaned records
    Returns:
        None
    """
    load_agribalyse_data_to_db(agribalyse_data)

if __name__ == "__main__":
    print("Fetching Agribalyse data from API...")
    etl_agribalyse()
    print("ETL Agribalyse completed.")
