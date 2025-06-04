import os
import sys
import psycopg2
import logging
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processing.utils import get_db_connection, safe_execute

def extract_openfoodfacts_chunks():
    url = "data/fr.openfoodfacts.org.products.csv"
    colonnes_utiles = [
        "code", "product_name", "generic_name", "brands", "categories", "labels_tags", "origins_tags", "packaging_tags", "countries_tags", "image_url",
        "energy-kcal_100g", "fat_100g", "saturated-fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
        "vitamin-c_100g", "vitamin-b12_100g", "vitamin-d_100g", "iron_100g", "calcium_100g",
        "nutriscore_score", "nutriscore_grade", "nova_group", "environmental_score_score", "environmental_score_grade",
        "ingredients_text", "ingredients_analysis_tags", "additives_tags", "allergens", "serving_size", "serving_quantity"
    ]
    rename_map = {
        "energy-kcal_100g": "energy_kcal_100g",
        "saturated-fat_100g": "saturated_fat_100g",
        "vitamin-c_100g": "vitamin_c_100g",
        "vitamin-b12_100g": "vitamin_b12_100g",
        "vitamin-d_100g": "vitamin_d_100g",
    }
    for chunk in pd.read_csv(url, nrows=2000000,sep="\t", encoding="utf-8", dtype={'code': str}, low_memory=False, on_bad_lines='skip', usecols=colonnes_utiles, chunksize=1000):
        chunk = chunk.rename(columns=rename_map)
        yield chunk

def load_openfoodfacts_chunk_to_db(chunk):
    conn = get_db_connection()
    if conn is None:
        return
    cur = conn.cursor()
    openfoodfacts_cols = [
        "code", "product_name", "generic_name", "brands", "categories", "labels_tags", "origins_tags", "packaging_tags", "image_url",
        "energy_kcal_100g", "fat_100g", "saturated_fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
        "vitamin_c_100g", "vitamin_b12_100g", "vitamin_d_100g", "iron_100g", "calcium_100g",
        "nutriscore_score", "nutriscore_grade", "nova_group", "environmental_score_score", "environmental_score_grade",
        "ingredients_text", "ingredients_analysis_tags", "additives_tags", "allergens", "serving_size", "serving_quantity"
    ]
    numeric_cols = [
        "energy_kcal_100g", "fat_100g", "saturated_fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g", "sodium_100g",
        "vitamin_c_100g", "vitamin_b12_100g", "vitamin_d_100g", "iron_100g", "calcium_100g",
        "nutriscore_score", "nova_group", "environmental_score_score"
    ]
    insert_rows = []
    for _, row in chunk.iterrows():
        if not isinstance(row.get('countries_tags'), str) or 'en:france' not in row['countries_tags']:
            continue
        name = row.get('product_name')
        code = row.get('code')
        if not isinstance(name, str) or not name.strip() or not code:
            continue
        # Insert into product_vector (toujours ligne à ligne pour récupérer l'id)
        safe_execute(cur, """
            INSERT INTO product_vector (name, source, code_source)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id;
        """, (name.strip(), 'openfoodfacts', code))
        result = cur.fetchone()
        if result:
            product_vector_id = result[0]
        else:
            safe_execute(cur, "SELECT id FROM product_vector WHERE name = %s AND source = %s;", (name.strip(), 'openfoodfacts'))
            fetch = cur.fetchone()
            if not fetch:
                continue
            product_vector_id = fetch[0]
        # Nettoyage des valeurs
        values = []
        for col in openfoodfacts_cols:
            val = row.get(col, None)
            if pd.isna(val):
                val = None
            if col in numeric_cols and val is not None:
                try:
                    val = float(val)
                except Exception:
                    val = None
            values.append(val)
        insert_rows.append([product_vector_id] + values)
    if insert_rows:
        sql = f"""
            INSERT INTO openfoodfacts (
                product_vector_id, {', '.join(openfoodfacts_cols)}
            ) VALUES (
                {', '.join(['%s'] * (1 + len(openfoodfacts_cols)))}
            ) ON CONFLICT DO NOTHING;
        """
        cur.executemany(sql, insert_rows)
    conn.commit()
    cur.close()
    conn.close()

def etl_openfoodfacts():
    chunk_count = 0
    for chunk in extract_openfoodfacts_chunks():
        load_openfoodfacts_chunk_to_db(chunk)
        chunk_count += 1
        if chunk_count % 1000 == 0:
            logging.info(f"Progression : {chunk_count} chunks traités")
    logging.info(f"Traitement terminé : {chunk_count} chunks traités ({chunk_count * 1000} lignes environ)")

if __name__ == "__main__":
    print("Lancement de l'ETL OpenFoodFacts...")
    etl_openfoodfacts()
    print("ETL OpenFoodFacts terminé.")
