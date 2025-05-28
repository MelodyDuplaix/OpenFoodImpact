import time
import requests

agribalyse_url = "https://data.ademe.fr/data-fair/api/v1/datasets/agribalyse-31-synthese/lines?after=1&page=1&size=24&sort=&select=Code_AGB,Code_CIQUAL,Groupe_d%27aliment,Sous-groupe_d%27aliment,Nom_du_Produit_en_Fran%C3%A7ais,LCI_Name,code_avion,Livraison,Approche_emballage_,Pr%C3%A9paration,Score_unique_EF,Changement_climatique,Appauvrissement_de_la_couche_d%27ozone,Rayonnements_ionisants,Formation_photochimique_d%27ozone,Particules_fines,Effets_toxicologiques_sur_la_sant%C3%A9_humaine___substances_non-canc%C3%A9rog%C3%A8nes,Effets_toxicologiques_sur_la_sant%C3%A9_humaine___substances_canc%C3%A9rog%C3%A8nes,Acidification_terrestre_et_eaux_douces,Eutrophisation_eaux_douces,Eutrophisation_marine,Eutrophisation_terrestre,%C3%89cotoxicit%C3%A9_pour_%C3%A9cosyst%C3%A8mes_aquatiques_d%27eau_douce,Utilisation_du_sol,%C3%89puisement_des_ressources_eau,%C3%89puisement_des_ressources_%C3%A9nerg%C3%A9tiques,%C3%89puisement_des_ressources_min%C3%A9raux,Changement_climatique_-_%C3%A9missions_biog%C3%A9niques,Changement_climatique_-_%C3%A9missions_fossiles,Changement_climatique_-_%C3%A9missions_li%C3%A9es_au_changement_d%27affectation_des_sols&format=json&q_mode=simple"

def get_agribalyse_data():
    """
    Fetches the Agribalyse data from the API and returns it as a JSON object.
    
    Returns:
        dict: The Agribalyse data.  Returns None if an error occurs.
    """
    start_time = time.time()
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
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Data fetched in {total_time:.2f} seconds")
        return all_data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    except (KeyError, ValueError) as e:
        print(f"Error parsing JSON response: {e}")
        return None

if __name__ == "__main__":
    print("Fetching Agribalyse data from API...")
    agribalyse_data = get_agribalyse_data()
    if agribalyse_data:
        print(f"Fetched {len(agribalyse_data)} records from Agribalyse API.")
    else:
        print("Failed to fetch Agribalyse data.")
