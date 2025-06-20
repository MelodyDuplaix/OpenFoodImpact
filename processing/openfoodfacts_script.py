import os
import sys
import logging
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processing.utils import get_db_connection, safe_execute, normalize_name, vectorize_name

openfoodfacts_url = "data/fr.openfoodfacts.org.products.csv"
# colonnes différentes entre celles présentes dans le CSV et celles utilisées dans la base de données
openfoodfact_csv_columns = [
    "code", "product_name", "brands", "categories", "labels_tags", "packaging_tags", "countries_tags", "image_url",
    "energy-kcal_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
    "vitamin-b12_100g", "vitamin-d_100g",
    "nutriscore_score", "nutriscore_grade", "nova_group", "environmental_score_score", "environmental_score_grade",
    "ingredients_text", "ingredients_analysis_tags", "additives_tags"
]
openfoodfact_columns = [
    "code", "product_name", "brands", "categories", "labels_tags", "packaging_tags", "image_url",
    "energy_kcal_100g", "fat_100g", "saturated_fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
    "nutriscore_score", "nutriscore_grade", "nova_group", "environmental_score_score", "environmental_score_grade",
    "ingredients_text", "ingredients_analysis_tags", "additives_tags"
]

def extract_openfoodfacts_chunks():
    """
    Lit le fichier CSV OpenFoodFacts par chunks.

    Args:
        None
    Returns:
        generator: Générateur de DataFrames pandas, chacun représentant un chunk du CSV.
    """
    rename_map = {
        "energy-kcal_100g": "energy_kcal_100g",
        "saturated-fat_100g": "saturated_fat_100g",
        "vitamin-b12_100g": "vitamin_b12_100g"
    }
    try:
        for chunk in pd.read_csv(openfoodfacts_url, nrows=200000, sep="\t", encoding="utf-8", dtype={'code': str}, 
                                 low_memory=True, on_bad_lines='skip', usecols=openfoodfact_csv_columns, chunksize=1000):
            chunk = chunk.rename(columns=rename_map)
            yield chunk
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des chunks OpenFoodFacts : {e}")
        return

def load_openfoodfacts_chunk_to_db(chunk):
    """
    Charge un chunk de données OpenFoodFacts dans la base PostgreSQL.

    Args:
        chunk (pd.DataFrame): DataFrame contenant un chunk de données OpenFoodFacts.
    Returns:
        None: Modifie la base de données.
    """
    conn = get_db_connection()
    if conn is None:
        logging.error("Connexion à la base impossible.")
        return
    cur = conn.cursor()
    # colonnes numériques à convertir en float
    numeric_cols = [
        "energy_kcal_100g", "fat_100g", "saturated_fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
        "nutriscore_score", "nova_group", "environmental_score_score"
    ]
    insert_rows = []
    for _, row in chunk.iterrows():
        try:
            for col in row.index:
                # il y avait plusieurs colonnes qui finissent par _grage et dont les valeurs inconnus étaient mis en 'unknown' ou 'not-applicable' au lieu de None
                if col.endswith('_grade') and str(row[col]).lower() in ['unknown', 'not-applicable']:
                    row[col] = None
            # on ne garde que les lignes avec moins de 80% de valeurs manquantes, pour avoir suffisamment de données
            if row.isna().mean() > 0.8:
                continue
            # on filtre pour ne garder que les produits français, pour que ca corresponde aux autres sources
            if not isinstance(row.get('countries_tags'), str) or 'en:france' not in row['countries_tags']:
                continue
            name = row.get('product_name')
            code = row.get('code')
            if not isinstance(name, str) or not name.strip() or not code:
                continue
            # on normalise et vectorise le nom pour pouvoir l'ajouter à product_vector
            name_normalized = normalize_name(name.strip())
            name_vector = vectorize_name(name_normalized)
            try:
                safe_execute(cur, """
                    INSERT INTO product_vector (name, name_vector, source, code_source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id;
                """, (name_normalized, name_vector, 'openfoodfacts', code))
                # on récupère l'id du produit pour l'insérer dans openfoodfacts
                # soit directement si l'insert a réussi, soit en le cherchant dans product_vector si le produit existait déjà
                result = cur.fetchone()
                if result:
                    product_vector_id = result[0]
                else:
                    safe_execute(cur, "SELECT id FROM product_vector WHERE name = %s AND source = %s;", (name_normalized, 'openfoodfacts'))
                    fetch = cur.fetchone()
                    if not fetch:
                        continue
                    product_vector_id = fetch[0]
            except Exception as e:
                logging.warning(f"Erreur insertion product_vector: {e}")
                continue
            values = []
            for col in openfoodfact_columns:
                # on boucle sur les colonnes pour récupérer les valeurs un convertissant les numériques en float et les null en None
                val = row.get(col, None)
                if pd.isna(val):
                    val = None
                if col in numeric_cols and val is not None:
                    try:
                        val = float(val)
                    except Exception:
                        val = None
                values.append(val)
            insert_rows.append([product_vector_id] + values) # on rajoute l'id de product_vector
        except Exception as e:
            logging.warning(f"Erreur lors du traitement d'une ligne OpenFoodFacts: {e}")
            continue
    if insert_rows:
        try:
            sql = f"""
                INSERT INTO openfoodfacts (
                    product_vector_id, {', '.join(openfoodfact_columns)}
                ) VALUES (
                    {', '.join(['%s'] * (1 + len(openfoodfact_columns)))}
                ) ON CONFLICT DO NOTHING;
            """
            cur.executemany(sql, insert_rows)
        except Exception as e:
            logging.error(f"Erreur lors de l'insertion batch OpenFoodFacts: {e}")
    conn.commit()
    cur.close()
    conn.close()

def pipeline_openfoodfacts():
    """
    Exécute le pipeline complet pour les données OpenFoodFacts.

    Args:
        None
    Returns:
        None: Les données sont extraites et chargées dans la base.
    """
    chunk_count = 0
    for chunk in extract_openfoodfacts_chunks():
        try:
            load_openfoodfacts_chunk_to_db(chunk)
        except Exception as e:
            logging.error(f"Erreur lors du traitement d'un chunk OpenFoodFacts: {e}")
        chunk_count += 1
        # on compte les chunks et on affiche un message tous les 1000 chunks pour suivre la progression
        if chunk_count % 1000 == 0:
            logging.info(f"Progression : {chunk_count} chunks traités")
    logging.info(f"Traitement terminé : {chunk_count} chunks traités ({chunk_count * 1000} lignes environ)")

if __name__ == "__main__":
    print("Lancement du pipeline OpenFoodFacts...")
    pipeline_openfoodfacts()
    print("pipeline OpenFoodFacts terminé.")
