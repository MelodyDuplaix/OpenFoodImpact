import logging
import re
import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processing.init_pgvector_tables import init_db
from processing.agribalyse_api import get_agribalyse_data, insert_agribalyse_data_to_db
from processing.openfoodfacts_script import load_openfoodfacts_chunk_to_db, etl_openfoodfacts
from processing.scraping_greenpeace import scrape_greenpeace_calendar, insert_season_data_to_db
from processing.scraping_marmiton import extract_all_recipes
from processing.clean_recipes_times import convert_recipe_times
import pandas as pd
import psycopg2
import pymongo

def is_db_filled():
    """
    Vérifie si la base de données PostgreSQL contient des données dans la table product_vector.

    Returns:
        bool: True si la table contient des données, False sinon.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB', 'postgres'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        cur = conn.cursor()
        cur.execute('SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);', ('product_vector',))
        exists_product_vector = cur.fetchone()[0] # type: ignore
        cur.execute('SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);', ('users',))
        exists_users = cur.fetchone()[0] # type: ignore
        if not exists_product_vector or not exists_users:
            cur.close()
            conn.close()
            return False
        cur.execute('SELECT COUNT(*) FROM product_vector;')
        result = cur.fetchone()
        count = result[0] if result else 0
        cur.close()
        conn.close()
        return count > 0
    except Exception as e:
        logging.warning(f"Impossible de vérifier la base : {e}")
        return False

def is_source_filled(table):
    """
    Vérifie si une source spécifique (table) est remplie dans la base de données PostgreSQL.

    Args:
        table (str): Nom de la table à vérifier.

    Returns:
        bool: True si la table contient des données, False sinon.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB', 'postgres'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        cur = conn.cursor()
        cur.execute(f'SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);', (table,))
        exists = cur.fetchone()[0] # type: ignore
        if not exists:
            cur.close()
            conn.close()
            return False
        cur.execute(f'SELECT COUNT(*) FROM {table};')
        result = cur.fetchone()
        count = result[0] if result else 0
        cur.close()
        conn.close()
        return count > 0
    except Exception as e:
        logging.warning(f"Impossible de vérifier la table {table} : {e}")
        return False

def is_marmiton_filled():
    """
    Vérifie si la base de données MongoDB (Marmiton) contient des données.

    Returns:
        bool: True si la collection contient des données, False sinon.
    """
    try:
        client = pymongo.MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
        db = client["OpenFoodImpact"]
        collection = db["recipes"]
        count = collection.estimated_document_count()
        client.close()
        return count > 0
    except Exception as e:
        logging.warning(f"Impossible de vérifier la base Marmiton (MongoDB) : {e}")
        return False

def main():
    """
    Fonction principale du pipeline de traitement des données.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    need_init_db = not is_db_filled()
    need_agribalyse = not is_source_filled('agribalyse')
    need_openfoodfacts = not is_source_filled('openfoodfacts')
    need_greenpeace = not is_source_filled('greenpeace_season')
    need_marmiton = not is_marmiton_filled()
    need_users = not is_source_filled('users')

    if not (need_agribalyse or need_openfoodfacts or need_greenpeace or need_marmiton or need_users):
        logging.info('Toutes les sources (Postgres + MongoDB Marmiton) sont déjà remplies. Arrêt du pipeline.')
        return
    try:
        if need_init_db:
            logging.info('Initialisation de la base de données...')
            init_db()
            logging.info('Base de données initialisée.')

        if need_agribalyse:
            start = time.time()
            logging.info('Récupération des données Agribalyse...')
            agribalyse_data = get_agribalyse_data()
            if agribalyse_data:
                logging.info(f'Insertion de {len(agribalyse_data)} lignes Agribalyse...')
                insert_agribalyse_data_to_db(agribalyse_data)
                logging.info('Données Agribalyse insérées.')
            else:
                logging.warning('Aucune donnée Agribalyse récupérée.')
            logging.info(f"Agribalyse traité en {time.time()-start:.2f} sec")
        else:
            logging.info('Données Agribalyse déjà présentes, skip.')

        if need_openfoodfacts:
            start = time.time()
            logging.info('Traitement OpenFoodFacts (tous les chunks)...')
            try:
                etl_openfoodfacts()
                logging.info('Tous les chunks OpenFoodFacts insérés.')
            except Exception as e:
                logging.error(f'Erreur lors du traitement complet OpenFoodFacts : {e}')
                logging.info('Tentative d’insertion du premier chunk uniquement...')
                off_path = os.path.join('data', 'fr.openfoodfacts.org.products.csv')
                colonnes_utiles = [
                    "code", "product_name", "generic_name", "brands", "categories", "labels_tags", "origins_tags", "packaging_tags", "countries_tags", "image_url",
                    "energy-kcal_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
                    "vitamin-c_100g", "vitamin-b12_100g", "vitamin-d_100g", "iron_100g", "calcium_100g", "nutriscore_score", "nutriscore_grade", "nova_group",
                    "environmental_score_score", "environmental_score_grade", "ingredients_text", "ingredients_analysis_tags", "additives_tags", "allergens", "serving_size", "serving_quantity"
                ]
                chunk_iter = pd.read_csv(off_path, sep="\t", encoding="utf-8", dtype={'code': str}, low_memory=False, on_bad_lines='skip', usecols=colonnes_utiles, chunksize=50)
                first_chunk = next(chunk_iter)
                load_openfoodfacts_chunk_to_db(first_chunk)
                logging.info('Premier chunk OpenFoodFacts inséré.')
            logging.info(f"OpenFoodFacts traité en {time.time()-start:.2f} sec")
        else:
            logging.info('Données OpenFoodFacts déjà présentes, skip.')

        if need_greenpeace:
            start = time.time()
            logging.info('Scraping calendrier Greenpeace...')
            calendar_data = scrape_greenpeace_calendar()
            insert_season_data_to_db(calendar_data)
            logging.info('Données Greenpeace insérées.')
            logging.info(f"Greenpeace traité en {time.time()-start:.2f} sec")
        else:
            logging.info('Données Greenpeace déjà présentes, skip.')

        if need_marmiton:
            start = time.time()
            logging.info('Scraping Marmiton...')
            recipes = extract_all_recipes()
            logging.info(f"{len(recipes)} recettes extraites et sauvegardées.")
            logging.info(f"Marmiton traité en {time.time()-start:.2f} sec")
            try:
                from processing.clean_marmiton_ingredients import extraire_ingredients_mongo, insert_ingredients_to_pgvector, update_recipes_with_normalized_ingredients
                logging.info('Nettoyage et insertion des ingrédients Marmiton dans product_vector...')
                ingredients_nettoyes = extraire_ingredients_mongo()
                insert_ingredients_to_pgvector(ingredients_nettoyes)
                update_recipes_with_normalized_ingredients()
                logging.info('Conversion des temps de recettes...')
                convert_recipe_times()
                logging.info('Conversion des temps de recettes terminée.')
                logging.info('Ingrédients Marmiton insérés et recettes mises à jour.')
            except Exception as e:
                logging.error(f'Erreur lors du nettoyage/insertion des ingrédients Marmiton : {e}')
        else:
            logging.info('Recettes Marmiton déjà extraites en base MongoDB, skip.')
        logging.info('Pipeline terminé avec succès.')
    except Exception as e:
        logging.error(f'Erreur pipeline: {e}')

if __name__ == '__main__':
    main()
