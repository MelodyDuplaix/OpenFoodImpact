import pandas as pd
import time

url = "https://fr.openfoodfacts.org/data/fr.openfoodfacts.org.products.csv"
chunksize = 1000
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

    # Illustrations
    "image_url",
    "image_ingredients_url",
    "image_nutrition_url",
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



if __name__ == "__main__":
    print("Processing Open Food Facts data...")
    process_data()
    print("Processing completed.")
