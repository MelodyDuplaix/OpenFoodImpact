import os
import sys
import psycopg2
import pandas as pd
import time
from .utils import get_db_connection, normalize_name, vectorize_name, safe_execute

# url = "https://fr.openfoodfacts.org/data/fr.openfoodfacts.org.products.csv"
url = ".data/fr.openfoodfacts.org.products.csv"
chunksize = 50
colonnes_utiles = [
    # Identifiants et métadonnées
    "code",
    "product_name",
    "generic_name",
    "brands",
    "categories",
    "labels_tags",
    "origins_tags",
    "packaging_tags",
    "countries_tags",
    "image_url",

    # Données nutritionnelles (pour 100g)
    "energy-kcal_100g",
    "fat_100g",
    "saturated-fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "fiber_100g",
    "proteins_100g",
    "salt_100g",
    "sodium_100g",
    "vitamin-c_100g",
    "vitamin-b12_100g",
    "vitamin-d_100g",
    "iron_100g",
    "calcium_100g",

    # Scores nutritionnels et environnementaux
    "nutriscore_score",
    "nutriscore_grade",
    "nova_group",
    "environmental_score_score",
    "environmental_score_grade",

    # Informations culinaires
    "ingredients_text",
    "ingredients_analysis_tags",
    "additives_tags",
    "allergens",
    "serving_size",
    "serving_quantity",
]

def process_data():
    start_time = time.time()
    total_lines = 0
    try:
        for chunk in pd.read_csv(url, sep="\t", encoding="utf-8", dtype={'code': str}, low_memory=False, on_bad_lines='skip', usecols=colonnes_utiles, chunksize=chunksize):
            total_lines += len(chunk)
    except pd.errors.EmptyDataError:
        print("The CSV file is empty.")
    except pd.errors.ParserError:
        print("Error parsing the CSV file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total number of lines processed: {total_lines}")
    print(f"Total processing time: {total_time:.2f} seconds")

def insert_openfoodfacts_chunk_to_db(chunk):
    conn = get_db_connection()
    cur = conn.cursor()
    for _, row in chunk.iterrows():
        name = row.get('product_name')
        if not isinstance(name, str) or not name.strip():
            continue
        name_normalized = normalize_name(name)
        name_vector = vectorize_name(name_normalized)
        safe_execute(cur, """
            INSERT INTO product_vector (name, name_vector, source)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id;
        """, (name_normalized, name_vector, 'openfoodfacts'))
        result = cur.fetchone()
        if result:
            product_vector_id = result[0]
        else:
            safe_execute(cur, "SELECT id FROM product_vector WHERE name = %s AND source = %s;", (name_normalized, 'openfoodfacts'))
            fetch = cur.fetchone()
            if not fetch:
                continue
            product_vector_id = fetch[0]
        columns = list(row.index)
        values = [row[col] for col in columns]
        columns.insert(0, 'product_vector_id')
        values.insert(0, product_vector_id)
        columns_escaped = [f'{col}' for col in columns]
        insert_sql = f"INSERT INTO openfoodfacts ({', '.join(columns_escaped)}) VALUES ({', '.join(['%s']*len(values))}) ON CONFLICT DO NOTHING;"
        safe_execute(cur, insert_sql, values)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    print("Processing Open Food Facts data...")
    chunk_iter = pd.read_csv(url, sep="\t", encoding="utf-8", dtype={'code': str}, low_memory=False, on_bad_lines='skip', usecols=colonnes_utiles, chunksize=chunksize, nrows=100)
    first_chunk = next(chunk_iter)
    print(f"Inserting first chunk of {len(first_chunk)} rows into database...")
    insert_openfoodfacts_chunk_to_db(first_chunk)
    print("First chunk inserted.")
    process_data()
    print("Processing completed.")
