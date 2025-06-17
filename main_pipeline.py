import logging
import re
import time
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processing.utils import get_db_connection
from processing.build_ingredient_links import create_ingredient_link_table, fill_ingredient_links
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

def are_recipes_parsed():
    """
    Vérifie si les recettes dans MongoDB ont été enrichies avec 'parsed_ingredients_details'.
    Regarde si au moins un document contient ce champ avec une liste non vide.

    Returns:
        bool: True si au moins une recette est parsée, False sinon.
    """
    try:
        client = pymongo.MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
        db = client["OpenFoodImpact"]
        collection = db["recipes"]
        # Cherche un document où 'parsed_ingredients_details' existe et n'est pas une liste vide
        # Si le champ n'existe pas, ou est null, ou est une liste vide, il ne sera pas compté.
        count = collection.count_documents({"parsed_ingredients_details": {"$exists": True, "$ne": []}})
        client.close()
        return count > 0
    except Exception as e:
        logging.warning(f"Impossible de vérifier si les recettes sont parsées (MongoDB) : {e}")
        return False # En cas d'erreur, on considère que ce n'est pas fait pour forcer le traitement.


def main():
    """
    Fonction principale du pipeline de traitement des données.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    need_init_db = not is_db_filled()
    need_agribalyse = not is_source_filled('agribalyse')
    need_openfoodfacts = not is_source_filled('openfoodfacts')
    need_greenpeace = not is_source_filled('greenpeace_season')
    marmiton_already_scraped = is_marmiton_filled()
    recipes_need_parsing = not are_recipes_parsed()
    need_users = not is_source_filled('users')
    need_ingredients_link = not is_source_filled('ingredient_link')

    # Condition pour relancer le traitement Marmiton :
    # - Soit la collection est vide (scraping initial)
    # - Soit la collection existe mais les recettes n'ont pas encore 'parsed_ingredients_details'
    need_marmiton_processing = not marmiton_already_scraped or recipes_need_parsing
    # On vérifie aussi need_init_db ici
    if not (need_init_db or need_agribalyse or need_openfoodfacts or need_greenpeace or need_marmiton_processing or need_users or need_ingredients_link):
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

        if not marmiton_already_scraped: # Si la collection est vide, on scrape
            start_scrape = time.time()
            logging.info('Scraping Marmiton (collection vide)...')
            extract_all_recipes() # Cette fonction insère directement dans MongoDB
            logging.info(f"Scraping Marmiton terminé en {time.time()-start_scrape:.2f} sec")
            marmiton_already_scraped = True # Marquer comme scrapé pour la suite

        if marmiton_already_scraped and recipes_need_parsing: # Si scrapé mais pas parsé, ou si on force le parsing
            # Cette condition signifie que les recettes existent, mais n'ont pas le champ `parsed_ingredients_details`.
            # On va donc exécuter le parsing et la mise à jour des recettes MongoDB.
            # On va aussi vérifier si de nouveaux ingrédients Marmiton doivent être ajoutés à product_vector.
            start = time.time()
            logging.info('Traitement des ingrédients Marmiton (parsing, normalisation, et mise à jour product_vector si besoin)...')
            try:
                from processing.clean_marmiton_ingredients import extraire_ingredients_mongo, insert_ingredients_to_pgvector, update_recipes_with_normalized_ingredients
                
                logging.info('Extraction des ingrédients Marmiton pour product_vector (nouveaux/mis à jour)...')
                df_ingredients_for_pv = extraire_ingredients_mongo() # Ne vectorise que les nouveaux
                if not df_ingredients_for_pv.empty:
                    logging.info(f'Insertion/Mise à jour de {len(df_ingredients_for_pv)} ingrédients Marmiton dans product_vector...')
                    insert_ingredients_to_pgvector(df_ingredients_for_pv)
                    need_ingredients_link = True # Si product_vector a été modifié, les liens doivent être revus
                else:
                    logging.info('Aucun nouvel ingrédient Marmiton à ajouter/mettre à jour dans product_vector.')

                logging.info('Mise à jour des recettes MongoDB avec parsed_ingredients_details...')
                update_recipes_with_normalized_ingredients()
                logging.info('Conversion des temps de recettes...')
                convert_recipe_times()
                logging.info('Conversion des temps de recettes terminée.')
                logging.info('Ingrédients Marmiton traités et recettes mises à jour avec parsed_ingredients_details.')
            except Exception: # Capture toutes les exceptions
                logging.error(f'Erreur lors du nettoyage/insertion des ingrédients Marmiton :', exc_info=True) # Ajout de exc_info=True
        elif marmiton_already_scraped and not recipes_need_parsing:
            logging.info('Recettes Marmiton déjà extraites et parsées, skip du traitement Marmiton.')
            
        if need_ingredients_link:
            start_link = time.time()
            logging.info('Création et remplissage de la table ingredient_link...')
            try:
                conn = get_db_connection()
                if not conn:
                    print("Connexion à la base impossible.")
                    return
                create_ingredient_link_table(conn)
                fill_ingredient_links(conn)
                conn.close()
                logging.info(f"Table ingredient_link créée et remplie avec succès en {time.time()-start_link:.2f} sec.")
            except Exception as e:
                logging.error(f'Erreur lors de la création/remplissage de la table ingredient_link : {e}')
                
        logging.info("Vérification et création de l'index texte pour MongoDB recipes...")
        mongo_client = None
        try:
            mongo_client = pymongo.MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
            db = mongo_client["OpenFoodImpact"]
            collection = db["recipes"]
            fields_for_text_index = [
                ("title", pymongo.TEXT),
                ("keywords", pymongo.TEXT),
                ("description", pymongo.TEXT)
            ]
            text_index_name = "recipes_content_text_search"

            existing_indexes = collection.index_information()
            if text_index_name in existing_indexes:
                current_key = sorted(existing_indexes[text_index_name]['key'])
                expected_key = sorted([(field, pymongo.TEXT) for field, _ in fields_for_text_index])
                if current_key == expected_key:
                    logging.info(f"L'index texte '{text_index_name}' existe déjà avec la bonne configuration.")
                else:
                    logging.warning(f"L'index texte '{text_index_name}' existe avec une configuration incorrecte. Il va être recréé.")
                    collection.drop_index(text_index_name)
                    logging.info(f"Ancien index '{text_index_name}' supprimé.")
                    collection.create_index(fields_for_text_index, name=text_index_name)
                    logging.info(f"Nouvel index texte '{text_index_name}' créé.")
            else:
                logging.info(f"Création de l'index texte '{text_index_name}' sur les champs: title, keywords, description.")
                collection.create_index(fields_for_text_index, name=text_index_name)
                logging.info(f"Index texte '{text_index_name}' créé avec succès.")
        except Exception as e_index:
            logging.error(f"Une erreur est survenue lors de la gestion de l'index texte MongoDB: {e_index}")
        finally:
            if mongo_client:
                mongo_client.close()
            

        logging.info('Pipeline terminé avec succès.')
    except Exception as e:
        logging.error(f'Erreur pipeline: {e}')

if __name__ == '__main__':
    main()
