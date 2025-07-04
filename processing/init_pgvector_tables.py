import os
from .utils import get_db_connection, safe_execute

def init_db():
    """Initialise les tables et index nécessaires dans la base PostgreSQL.

    Returns:
        None
    """
    conn = get_db_connection()
    if conn is None:
        raise RuntimeError("Database connection could not be established.")
    cur = conn.cursor()
    # on active les extensions nécessaires
    safe_execute(cur, "CREATE EXTENSION IF NOT EXISTS vector;")
    safe_execute(cur, "CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS product_vector (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        name_vector vector(384),
        source VARCHAR(32) NOT NULL
        -- code_source supprimé
    );''')
    # on créer des index pour optimiser les recherches
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_product_vector_name ON product_vector (name);")
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_gin_product_vector_name ON product_vector USING gin (name gin_trgm_ops);")
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_product_vector_source ON product_vector (source);")
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_hnsw_product_vector_name_vector ON product_vector USING hnsw (name_vector vector_cosine_ops);")
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS agribalyse (
        id SERIAL PRIMARY KEY,
        product_vector_id INTEGER REFERENCES product_vector(id),
        code_agb TEXT,
        code_ciqual TEXT,
        lci_name TEXT,
        nom_produit_francais TEXT,
        changement_climatique FLOAT,
        score_unique_ef FLOAT,
        ecotoxicite_eau_douce FLOAT,
        epuisement_ressources_energetiques FLOAT,
        eutrophisation_marine FLOAT,
        sous_groupe_aliment TEXT,
        effets_tox_cancerogenes FLOAT,
        approche_emballage TEXT,
        epuisement_ressources_eau FLOAT,
        eutrophisation_terrestre FLOAT,
        utilisation_sol FLOAT,
        code_avion TEXT,
        effets_tox_non_cancerogenes FLOAT,
        epuisement_ressources_mineraux FLOAT,
        particules_fines FLOAT,
        formation_photochimique_ozone FLOAT,
        livraison TEXT,
        preparation TEXT,
        changement_climatique_biogenique FLOAT,
        acidification_terrestre_eaux_douces FLOAT,
        groupe_aliment TEXT,
        changement_climatique_cas FLOAT,
        appauvrissement_couche_ozone FLOAT,
        rayonnements_ionisants FLOAT,
        eutrophisation_eaux_douces FLOAT,
        changement_climatique_fossile FLOAT
    );''')
    # idem, on a besoin d'index pour optimiser les recherches
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_agribalyse_code_agb ON agribalyse (code_agb);")
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_agribalyse_code_ciqual ON agribalyse (code_ciqual);")
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_agribalyse_product_vector_id ON agribalyse (product_vector_id);")
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS openfoodfacts (
        id SERIAL PRIMARY KEY,
        product_vector_id INTEGER REFERENCES product_vector(id),
        code TEXT,
        product_name TEXT,
        brands TEXT,
        categories TEXT,
        labels_tags TEXT,
        packaging_tags TEXT,
        image_url TEXT,
        energy_kcal_100g FLOAT,
        fat_100g FLOAT,
        saturated_fat_100g FLOAT,
        carbohydrates_100g FLOAT,
        sugars_100g FLOAT,
        fiber_100g FLOAT,
        proteins_100g FLOAT,
        salt_100g FLOAT,
        nutriscore_score FLOAT,
        nutriscore_grade TEXT,
        nova_group INTEGER,
        environmental_score_score FLOAT,
        environmental_score_grade TEXT,
        ingredients_text TEXT,
        ingredients_analysis_tags TEXT,
        additives_tags TEXT,
        sodium_100g FLOAT
    );''')
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_openfoodfacts_code ON openfoodfacts (code);")
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_openfoodfacts_product_name ON openfoodfacts (product_name);")
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_openfoodfacts_product_vector_id ON openfoodfacts (product_vector_id);")
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS greenpeace_season (
        id SERIAL PRIMARY KEY,
        product_vector_id INTEGER REFERENCES product_vector(id),
        month TEXT
    );''')
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_greenpeace_season_product_vector_id ON greenpeace_season (product_vector_id);")
    safe_execute(cur, '''
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        user_level TEXT NOT NULL DEFAULT 'user'
    );''')
    safe_execute(cur, "CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);")
    conn.commit()
    cur.close()
    conn.close()
