import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processing.init_pgvector_tables import init_db
from processing.agribalyse_api import get_agribalyse_data, insert_agribalyse_data_to_db
from processing.openfoodfacts_script import insert_openfoodfacts_chunk_to_db
from processing.scraping_greenpeace import scrape_greenpeace_calendar, insert_season_data_to_db
import pandas as pd
import os

def main():
    logging.basicConfig(level=logging.INFO)
    try:
        logging.info('Initialisation de la base de données...')
        init_db()
        logging.info('Base de données initialisée.')

        # Agribalyse
        logging.info('Récupération des données Agribalyse...')
        agribalyse_data = get_agribalyse_data()
        if agribalyse_data:
            logging.info(f'Insertion de {len(agribalyse_data)} lignes Agribalyse...')
            insert_agribalyse_data_to_db(agribalyse_data)
            logging.info('Données Agribalyse insérées.')
        else:
            logging.warning('Aucune donnée Agribalyse récupérée.')

        # OpenFoodFacts (premier chunk seulement pour la démo)
        # logging.info('Traitement OpenFoodFacts (premier chunk)...')
        # off_path = os.path.join('data', 'fr.openfoodfacts.org.products.csv')
        # colonnes_utiles = [
        #     "code", "product_name", "generic_name", "brands", "categories", "labels_tags", "origins_tags", "packaging_tags", "countries_tags", "image_url",
        #     "energy-kcal_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
        #     "vitamin-c_100g", "vitamin-b12_100g", "vitamin-d_100g", "iron_100g", "calcium_100g", "nutriscore_score", "nutriscore_grade", "nova_group",
        #     "environmental_score_score", "environmental_score_grade", "ingredients_text", "ingredients_analysis_tags", "additives_tags", "allergens", "serving_size", "serving_quantity"
        # ]
        # chunk_iter = pd.read_csv(off_path, sep="\t", encoding="utf-8", dtype={'code': str}, low_memory=False, on_bad_lines='skip', usecols=colonnes_utiles, chunksize=50)
        # first_chunk = next(chunk_iter)
        # insert_openfoodfacts_chunk_to_db(first_chunk)
        # logging.info('Premier chunk OpenFoodFacts inséré.')

        # Greenpeace
        logging.info('Scraping calendrier Greenpeace...')
        calendar_data = scrape_greenpeace_calendar()
        insert_season_data_to_db(calendar_data)
        logging.info('Données Greenpeace insérées.')

        logging.info('Pipeline terminé avec succès.')
    except Exception as e:
        logging.error(f'Erreur pipeline: {e}')

if __name__ == '__main__':
    main()
