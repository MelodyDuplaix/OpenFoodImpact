import pandas as pd
from pymongo import MongoClient
import re
import os
import unicodedata
import sys
import psycopg2
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processing.utils import vectorize_name, normalize_name, get_db_connection, safe_execute

def extraire_ingredients_mongo():
    """
    Extrait les ingrédients des recettes stockées dans MongoDB et les normalise.

    Returns:
        pd.DataFrame: DataFrame contenant les ingrédients extraits et normalisés.
    """
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
    db = client["OpenFoodImpact"]
    collection = db["recipes"]
    ingredients_set = set()
    for doc in collection.find({}, {"recipeIngredient": 1}):
        if "recipeIngredient" in doc and isinstance(doc["recipeIngredient"], list):
            for item in doc["recipeIngredient"]:
                ingr = normalize_name(item)
                if ingr:
                    ingredients_set.add(ingr)
    df = pd.DataFrame(list(ingredients_set), columns=["name"])
    df["source"] = "marmiton"
    df["name_vector"] = df["name"].apply(vectorize_name)
    df["code_source"] = None
    client.close()
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
    for _, row in df.iterrows():
        try:
            safe_execute(cur, """
                INSERT INTO product_vector (name, name_vector, source, code_source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (row["name"], row["name_vector"], row["source"], row["code_source"]))
        except Exception as e:
            print(f"Erreur insertion ingredient: {e}")
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
    for doc in collection.find({}, {"_id": 1, "recipeIngredient": 1}):
        if "recipeIngredient" in doc and isinstance(doc["recipeIngredient"], list):
            normalized = [normalize_name(item) for item in doc["recipeIngredient"] if normalize_name(item)]
            collection.update_one({"_id": doc["_id"]}, {"$set": {"normalized_ingredients": normalized}})
    client.close()

if __name__ == "__main__":
    ingredients_nettoyes = extraire_ingredients_mongo()
    print("Insertion des ingrédients dans product_vector...")
    insert_ingredients_to_pgvector(ingredients_nettoyes)
    print("Mise à jour des recettes avec les ingrédients normalisés...")
    update_recipes_with_normalized_ingredients()
    print("Nombre d'ingrédients uniques :", len(ingredients_nettoyes))
    print(ingredients_nettoyes.head(50))
    print(ingredients_nettoyes.tail(50))
    print("Dernier ingrédient :", ingredients_nettoyes["name"].iloc[-1])
