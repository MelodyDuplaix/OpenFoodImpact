import pandas as pd
from pymongo import MongoClient
import re
import os
import unicodedata
import sys
import psycopg2
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processing.utils import vectorize_name, normalize_name, get_db_connection, safe_execute, parse_ingredient_details_fr_en, DEFAULT_QUANTITY_GRAMS

def extraire_ingredients_mongo():
    """
    Extrait les ingrédients uniques des recettes Marmiton (MongoDB) qui ne sont pas encore
    dans product_vector ou pour lesquels une mise à jour pourrait être nécessaire.
    Prépare un DataFrame pour l'insertion/mise à jour dans product_vector.
    Returns:
        pd.DataFrame: DataFrame contenant les ingrédients extraits et normalisés.
    """
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
    db = client["OpenFoodImpact"]
    collection = db["recipes"]
    
    # Optionnel: Récupérer les ingrédients Marmiton déjà dans product_vector pour éviter de les retraiter inutilement
    # Cela suppose que si un nom normalisé est dans product_vector, son vecteur est correct.
    existing_marmiton_ingredients_in_pv = set()
    conn_pg = get_db_connection()
    if conn_pg:
        cur_pg = conn_pg.cursor()
        try:
            cur_pg.execute("SELECT name FROM product_vector WHERE source = 'marmiton';")
            for row in cur_pg.fetchall():
                existing_marmiton_ingredients_in_pv.add(row[0])
        except Exception as e:
            print(f"Erreur lors de la récupération des ingrédients Marmiton existants de product_vector: {e}")
        finally:
            cur_pg.close()
            conn_pg.close()

    new_or_updated_ingredients_for_pv = set()

    # On parcourt les recettes pour identifier les ingrédients à potentiellement ajouter/mettre à jour dans product_vector
    # Cette partie est coûteuse si on la fait à chaque fois.
    # Idéalement, on ne la ferait que si on suspecte des changements majeurs.
    # Pour l'instant, on la garde pour s'assurer que product_vector est complet pour Marmiton.
    for doc in collection.find({}, {"recipeIngredient": 1, "_id": 0}): # _id: 0 pour ne pas le charger inutilement ici
        if "recipeIngredient" in doc and isinstance(doc["recipeIngredient"], list):
            for item in doc["recipeIngredient"]:
                if not isinstance(item, str): # S'assurer que 'item' est une chaîne
                    # logging.debug(f"Skipping non-string ingredient item: {item}") # Décommenter pour le débogage
                    continue
                parsed_info = parse_ingredient_details_fr_en(item)
                # Vérification supplémentaire pour s'assurer que parsed_name est une chaîne
                parsed_name_value = parsed_info.get("parsed_name")
                normalized_for_pv = normalize_name(parsed_name_value)
                if normalized_for_pv:
                    # On ne vectorise que si l'ingrédient n'est pas déjà dans product_vector
                    # ou si on voulait forcer une mise à jour (logique plus complexe non implémentée ici)
                    if normalized_for_pv not in existing_marmiton_ingredients_in_pv:
                        new_or_updated_ingredients_for_pv.add(normalized_for_pv)

    client.close()

    df = pd.DataFrame(list(new_or_updated_ingredients_for_pv), columns=["name"])
    df["source"] = "marmiton"
    if not df.empty:
        df["name_vector"] = df["name"].apply(vectorize_name)
    else: # Éviter l'erreur avec apply sur un DataFrame vide
        df["name_vector"] = pd.Series(dtype='object')
    df["code_source"] = None
    return df

def insert_ingredients_to_pgvector(df):
    """
    Insère les ingrédients normalisés dans la table product_vector de PostgreSQL.
    
    Args:
        df (pd.DataFrame): DataFrame contenant les ingrédients à insérer.
    Returns:
        None
    """
    conn = get_db_connection()
    if conn is None:
        print("Erreur : connexion à la base Postgres impossible pour l'insertion des ingrédients.")
        return
    cur = conn.cursor()
    if not df.empty: # S'assurer qu'il y a des données à insérer
        for _, row in df.iterrows():
            try:
                safe_execute(cur, """
                    INSERT INTO product_vector (name, name_vector, source, code_source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name, source) DO NOTHING; 
                """, (row["name"], row["name_vector"], row["source"], row["code_source"])) # Ajout de ON CONFLICT (name, source)
            except Exception as e:
                print(f"Erreur insertion ingredient: {e}") # Devrait être un logging.error
                continue
        conn.commit()
    cur.close()
    conn.close()

def update_recipes_with_normalized_ingredients():
    """
    Met à jour les recettes dans MongoDB en ajoutant une liste d'ingrédients normalisés.
    """
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
    db = client["OpenFoodImpact"]
    collection = db["recipes"]
    # On ne met à jour que les documents qui n'ont pas encore parsed_ingredients_details ou si on veut forcer
    for doc in collection.find({}, {"recipeIngredient": 1, "_id": 1}):
        if "recipeIngredient" in doc and isinstance(doc["recipeIngredient"], list):
            parsed_details_for_this_recipe = []
            normalized_ingredient_names_for_search = [] # L'ancien champ "normalized_ingredients"
            for item_string in doc["recipeIngredient"]:
                if not isinstance(item_string, str): # Ensure 'item_string' is a string
                    continue
                parsed = parse_ingredient_details_fr_en(item_string)
                parsed["normalized_name_for_matching"] = normalize_name(parsed["parsed_name"])
                if parsed["normalized_name_for_matching"]: # S'assurer qu'il y a un nom après normalisation
                    normalized_ingredient_names_for_search.append(parsed["normalized_name_for_matching"])
                parsed_details_for_this_recipe.append(parsed)
            collection.update_one({"_id": doc["_id"]}, {"$set": {"parsed_ingredients_details": parsed_details_for_this_recipe, "normalized_ingredients": normalized_ingredient_names_for_search}})
    client.close()

if __name__ == "__main__":
    # Cette partie est pour le test direct du script, le pipeline principal gère l'appel
    print("Extraction des ingrédients Marmiton pour product_vector (nouveaux ou mis à jour)...")
    df_ingredients_for_pv = extraire_ingredients_mongo()
    if not df_ingredients_for_pv.empty:
        print(f"Insertion de {len(df_ingredients_for_pv)} ingrédients dans product_vector...")
        insert_ingredients_to_pgvector(df_ingredients_for_pv)
    else:
        print("Aucun nouvel ingrédient Marmiton à ajouter/mettre à jour dans product_vector.")
    
    print("Mise à jour des recettes avec les ingrédients normalisés...")
    update_recipes_with_normalized_ingredients()
