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

def extract_ingredients_mongo():
    """
    Extrait les ingrédients uniques des recettes Marmiton pour product_vector.

    Args:
        None
    Returns:
        pd.DataFrame: DataFrame des ingrédients Marmiton normalisés et vectorisés.
    """
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
    db = client["OpenFoodImpact"]
    collection = db["recipes"]
    
    existing_marmiton_ingredients_in_pv = set()
    conn_pg = get_db_connection()
    # on récupère tous les ingrédients déja en table product_vector
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

    new_or_updated_ingredients_for_pv = set() # on crée un set pour récupérer les ingrédients sans doublons, et donc optimiser le traitement

    # on parcourt les recettes en récupérant leurs ingrédients
    for doc in collection.find({}, {"recipeIngredient": 1, "_id": 0}):
        if "recipeIngredient" in doc and isinstance(doc["recipeIngredient"], list):
            for item in doc["recipeIngredient"]:
                if not isinstance(item, str):
                    continue
                # pour chaque ingrédient, on le parse et on le normalise pour plus tard l'insérer dans product_vector si il n'est pas déjà présent
                parsed_info = parse_ingredient_details_fr_en(item)
                parsed_name_value = parsed_info.get("parsed_name")
                normalized_for_pv = normalize_name(parsed_name_value)
                if normalized_for_pv:
                    if normalized_for_pv not in existing_marmiton_ingredients_in_pv:
                        new_or_updated_ingredients_for_pv.add(normalized_for_pv)

    client.close()

    # on crée un DataFrame avec les ingrédients à insérer, et on ajoute le vecteur du nom
    df = pd.DataFrame(list(new_or_updated_ingredients_for_pv), columns=["name"])
    df["source"] = "marmiton"
    if not df.empty:
        df["name_vector"] = df["name"].apply(vectorize_name)
    else:
        df["name_vector"] = pd.Series(dtype='object')
    df["code_source"] = None
    return df

def insert_ingredients_to_pgvector(df):
    """
    Insère les ingrédients d'un DataFrame dans la table product_vector.

    Args:
        df (pd.DataFrame): DataFrame contenant les ingrédients à insérer.
    Returns:
        None: Modifie la base de données.
    """
    conn = get_db_connection()
    if conn is None:
        print("Erreur : connexion à la base Postgres impossible pour l'insertion des ingrédients.")
        return
    cur = conn.cursor()
    if not df.empty:
        for _, row in df.iterrows():
            try:
                safe_execute(cur, """
                    INSERT INTO product_vector (name, name_vector, source, code_source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (name, source) DO NOTHING; 
                """, (row["name"], row["name_vector"], row["source"], row["code_source"]))
            except Exception as e:
                print(f"Erreur insertion ingredient: {e}")
                continue
        conn.commit()
    cur.close()
    conn.close()

def update_recipes_with_normalized_ingredients():
    """
    Met à jour les recettes MongoDB avec les détails des ingrédients parsés et normalisés.

    Args:
        None
    Returns:
        None: Modifie la base de données MongoDB.
    """
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
    db = client["OpenFoodImpact"]
    collection = db["recipes"]
    # on parcourt les recettes pour extraire, parser et normaliser les ingrédients
    for doc in collection.find({}, {"recipeIngredient": 1, "_id": 1}):
        if "recipeIngredient" in doc and isinstance(doc["recipeIngredient"], list):
            parsed_details_for_this_recipe = []
            normalized_ingredient_names_for_search = []
            for item_string in doc["recipeIngredient"]:
                if not isinstance(item_string, str):
                    continue
                parsed = parse_ingredient_details_fr_en(item_string)
                parsed["normalized_name_for_matching"] = normalize_name(parsed["parsed_name"])
                if parsed["normalized_name_for_matching"]:
                    normalized_ingredient_names_for_search.append(parsed["normalized_name_for_matching"])
                parsed_details_for_this_recipe.append(parsed)
            # on met à jour la recette avec les détails des ingrédients parsés et les noms normalisés
            collection.update_one({"_id": doc["_id"]}, {"$set": {"parsed_ingredients_details": parsed_details_for_this_recipe, "normalized_ingredients": normalized_ingredient_names_for_search}})
    client.close()

if __name__ == "__main__":
    print("Extraction des ingrédients Marmiton pour product_vector (nouveaux ou mis à jour)...")
    df_ingredients_for_pv = extract_ingredients_mongo()
    if not df_ingredients_for_pv.empty:
        print(f"Insertion de {len(df_ingredients_for_pv)} ingrédients dans product_vector...")
        insert_ingredients_to_pgvector(df_ingredients_for_pv)
    else:
        print("Aucun nouvel ingrédient Marmiton à ajouter/mettre à jour dans product_vector.")
    
    print("Mise à jour des recettes avec les ingrédients normalisés...")
    update_recipes_with_normalized_ingredients()
