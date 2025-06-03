import os
from .utils import get_db_connection, safe_execute

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    safe_execute(cur, "CREATE EXTENSION IF NOT EXISTS vector;")
    safe_execute(cur, "CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS product_vector (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        name_vector VECTOR(384),
        source VARCHAR(32) NOT NULL,
        UNIQUE (name, source)
    );''')
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS agribalyse (
        id SERIAL PRIMARY KEY,
        product_vector_id INTEGER REFERENCES product_vector(id),
        ecotoxicite_eau_douce FLOAT,
        code_agb TEXT,
        epuisement_ressources_energetiques FLOAT,
        eutrophisation_marine FLOAT,
        sous_groupe_aliment TEXT,
        effets_tox_cancerogenes FLOAT,
        approche_emballage TEXT,
        code_ciqual TEXT,
        lci_name TEXT,
        nom_produit_francais TEXT,
        epuisement_ressources_eau FLOAT,
        eutrophisation_terrestre FLOAT,
        utilisation_sol FLOAT,
        code_avion TEXT,
        effets_tox_non_cancerogenes FLOAT,
        changement_climatique FLOAT,
        epuisement_ressources_mineraux FLOAT,
        particules_fines FLOAT,
        formation_photochimique_ozone FLOAT,
        livraison TEXT,
        preparation TEXT,
        changement_climatique_biogenique FLOAT,
        acidification_terrestre_eaux_douces FLOAT,
        groupe_aliment TEXT,
        changement_climatique_cas FLOAT,
        score_unique_ef FLOAT,
        appauvrissement_couche_ozone FLOAT,
        rayonnements_ionisants FLOAT,
        eutrophisation_eaux_douces FLOAT,
        changement_climatique_fossile FLOAT,
        score FLOAT
    );''')
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS openfoodfacts (
        id SERIAL PRIMARY KEY,
        product_vector_id INTEGER REFERENCES product_vector(id),
        code TEXT,
        product_name TEXT,
        generic_name TEXT,
        brands TEXT,
        categories TEXT,
        labels_tags TEXT,
        origins_tags TEXT,
        packaging_tags TEXT,
        countries_tags TEXT,
        image_url TEXT,
        energy_kcal_100g FLOAT,
        fat_100g FLOAT,
        saturated_fat_100g FLOAT,
        carbohydrates_100g FLOAT,
        sugars_100g FLOAT,
        fiber_100g FLOAT,
        proteins_100g FLOAT,
        salt_100g FLOAT,
        sodium_100g FLOAT,
        vitamin_c_100g FLOAT,
        vitamin_b12_100g FLOAT,
        vitamin_d_100g FLOAT,
        iron_100g FLOAT,
        calcium_100g FLOAT,
        nutriscore_score FLOAT,
        nutriscore_grade TEXT,
        nova_group INTEGER,
        environmental_score_score FLOAT,
        environmental_score_grade TEXT,
        ingredients_text TEXT,
        ingredients_analysis_tags TEXT,
        additives_tags TEXT,
        allergens TEXT,
        serving_size TEXT,
        serving_quantity FLOAT
    );''')
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS greenpeace_season (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        product_vector_id INTEGER REFERENCES product_vector(id),
        month VARCHAR(16)
    );''')
    conn.commit()
    cur.close()
    conn.close()
