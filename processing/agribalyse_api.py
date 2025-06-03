import time
import requests
import os
import sys
import psycopg2
import unicodedata
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processing.scraping_greenpeace import vectorize_product_name, normalize_name
from .utils import get_db_connection, normalize_name, vectorize_name, safe_execute

agribalyse_url = "https://data.ademe.fr/data-fair/api/v1/datasets/agribalyse-31-synthese/lines?after=1&page=1&size=24&sort=&select=Code_AGB,Code_CIQUAL,Groupe_d%27aliment,Sous-groupe_d%27aliment,Nom_du_Produit_en_Fran%C3%A7ais,LCI_Name,code_avion,Livraison,Approche_emballage_,Pr%C3%A9paration,Score_unique_EF,Changement_climatique,Appauvrissement_de_la_couche_d%27ozone,Rayonnements_ionisants,Formation_photochimique_d%27ozone,Particules_fines,Effets_toxicologiques_sur_la_sant%C3%A9_humaine___substances_non-canc%C3%A9rog%C3%A8nes,Effets_toxicologiques_sur_la_sant%C3%A9_humaine___substances_canc%C3%A9rog%C3%A8nes,Acidification_terrestre_et_eaux_douces,Eutrophisation_eaux_douces,Eutrophisation_marine,Eutrophisation_terrestre,%C3%89cotoxicit%C3%A9_pour_%C3%A9cosyst%C3%A8mes_aquatiques_d%27eau_douce,Utilisation_du_sol,%C3%89puisement_des_ressources_eau,%C3%89puisement_des_ressources_%C3%A9nerg%C3%A9tiques,%C3%89puisement_des_ressources_min%C3%A9raux,Changement_climatique_-_%C3%A9missions_biog%C3%A9niques,Changement_climatique_-_%C3%A9missions_fossiles,Changement_climatique_-_%C3%A9missions_li%C3%A9es_au_changement_d%27affectation_des_sols&format=json&q_mode=simple"

def get_agribalyse_data():
    """
    Fetches the Agribalyse data from the API and returns it as a JSON object.
    
    Returns:
        dict: The Agribalyse data.  Returns None if an error occurs.
    """
    all_data = []
    try:
        response = requests.get(agribalyse_url)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        all_data.extend(data["results"])
        while data.get("next", None):
            next_url = data["next"]
            response = requests.get(next_url)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data["results"])
        return all_data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    except (KeyError, ValueError) as e:
        print(f"Error parsing JSON response: {e}")
        return None


def insert_agribalyse_data_to_db(agribalyse_data):
    """
    Inserts the Agribalyse data into the PostgreSQL database, vectorizing product names and linking to product_vector.
    
    Args:
        agribalyse_data (list): The list of Agribalyse data records.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    for record in agribalyse_data:
        # Mapping des clés de l'API vers les nouveaux noms de colonnes sans caractères spéciaux
        mapping = {
            "Écotoxicité_pour_écosystèmes_aquatiques_d'eau_douce": "ecotoxicite_eau_douce",
            "Code_AGB": "code_agb",
            "Épuisement_des_ressources_énergétiques": "epuisement_ressources_energetiques",
            "Eutrophisation_marine": "eutrophisation_marine",
            "Sous-groupe_d'aliment": "sous_groupe_aliment",
            "Effets_toxicologiques_sur_la_santé_humaine___substances_cancérogènes": "effets_tox_cancerogenes",
            "Approche_emballage_": "approche_emballage",
            "Code_CIQUAL": "code_ciqual",
            "LCI_Name": "lci_name",
            "Nom_du_Produit_en_Français": "nom_produit_francais",
            "Épuisement_des_ressources_eau": "epuisement_ressources_eau",
            "Eutrophisation_terrestre": "eutrophisation_terrestre",
            "Utilisation_du_sol": "utilisation_sol",
            "code_avion": "code_avion",
            "Effets_toxicologiques_sur_la_santé_humaine___substances_non-cancérogènes": "effets_tox_non_cancerogenes",
            "Changement_climatique": "changement_climatique",
            "Épuisement_des_ressources_minéraux": "epuisement_ressources_mineraux",
            "Particules_fines": "particules_fines",
            "Formation_photochimique_d'ozone": "formation_photochimique_ozone",
            "Livraison": "livraison",
            "Préparation": "preparation",
            "Changement_climatique_-_émissions_biogéniques": "changement_climatique_biogenique",
            "Acidification_terrestre_et_eaux_douces": "acidification_terrestre_eaux_douces",
            "Groupe_d'aliment": "groupe_aliment",
            "Changement_climatique_-_émissions_liées_au_changement_d'affectation_des_sols": "changement_climatique_cas",
            "Score_unique_EF": "score_unique_ef",
            "Appauvrissement_de_la_couche_d'ozone": "appauvrissement_couche_ozone",
            "Rayonnements_ionisants": "rayonnements_ionisants",
            "Eutrophisation_eaux_douces": "eutrophisation_eaux_douces",
            "Changement_climatique_-_émissions_fossiles": "changement_climatique_fossile",
            "_score": "score"
        }
        # Remap record keys
        record_clean = {mapping.get(k, k): v for k, v in record.items()}
        name = record_clean.get('nom_produit_francais')
        if not name:
            continue
        name_normalized = normalize_name(name)
        name_vector = vectorize_name(name_normalized)
        safe_execute(cur, """
            INSERT INTO product_vector (name, name_vector, source)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id;
        """, (name_normalized, name_vector, 'agribalyse'))
        result = cur.fetchone()
        if result:
            product_vector_id = result[0]
        else:
            safe_execute(cur, "SELECT id FROM product_vector WHERE name = %s AND source = %s;", (name_normalized, 'agribalyse'))
            fetch = cur.fetchone()
            if not fetch:
                continue
            product_vector_id = fetch[0]
        columns = list(record_clean.keys())
        values = [record_clean[col] for col in columns]
        columns.insert(0, 'product_vector_id')
        values.insert(0, product_vector_id)
        columns_escaped = [f'{col}' for col in columns]
        insert_sql = f"INSERT INTO agribalyse ({', '.join(columns_escaped)}) VALUES ({', '.join(['%s']*len(values))}) ON CONFLICT DO NOTHING;"
        safe_execute(cur, insert_sql, values)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    print("Fetching Agribalyse data from API...")
    agribalyse_data = get_agribalyse_data()
    if agribalyse_data:
        print(f"Fetched {len(agribalyse_data)} records from Agribalyse API.")
        print("Inserting into database...")
        insert_agribalyse_data_to_db(agribalyse_data)
        print("Insertion completed.")
    else:
        print("Failed to fetch Agribalyse data.")
