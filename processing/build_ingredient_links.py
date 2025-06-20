from processing.ingredient_similarity import find_similar_ingredients
from processing.utils import get_db_connection

def create_ingredient_link_table(conn):
    """
    Crée la table 'ingredient_link' et ses index si elle n'existe pas.

    Args:
        conn (psycopg2.extensions.connection): Connexion à la base de données PostgreSQL.
    Returns:
        None: La fonction modifie la base de données directement.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingredient_link (
            id SERIAL PRIMARY KEY,
            id_source INTEGER REFERENCES product_vector(id),
            source TEXT,
            id_linked INTEGER REFERENCES product_vector(id),
            linked_source TEXT,
            score FLOAT,
            UNIQUE (id_source, source, id_linked, linked_source)
        );
    """)
    # les index serviront à accélérer les requêtes de recherche de liens entre ingrédients, surtout si l'on cherche pour beaucoup de recettes dans une même requête
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_link_id_source ON ingredient_link (id_source);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_link_id_linked ON ingredient_link (id_linked);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_link_source_text ON ingredient_link (source);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_link_linked_source ON ingredient_link (linked_source);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_link_score ON ingredient_link (score DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_link_id_source_linked_source_score ON ingredient_link (id_source, linked_source, score DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ingredient_link_id_linked_source_text_score ON ingredient_link (id_linked, source, score DESC);")
    conn.commit()
    cur.close()

def fill_ingredient_links(conn):
    """
    Remplit la table 'ingredient_link' avec les liens entre ingrédients similaires.

    Args:
        conn (psycopg2.extensions.connection): Connexion à la base de données PostgreSQL.
    Returns:
        None: La fonction modifie la base de données directement.
    """
    cur = conn.cursor()
    cur.execute("SELECT id, name, source FROM product_vector;")
    all_products = cur.fetchall()
    for prod_id, name, source in all_products:
        # on boucle sur tous les produits dans product_vector pour chercher les ingrédients similaires à ce produit des autres sources
        similars = find_similar_ingredients(name, source, conn)
        # pour chaque ingrédient similaire trouvé, on insère un lien dans la table ingredient_link
        for other_source, match in similars.items():
            cur.execute("""
                INSERT INTO ingredient_link (id_source, source, id_linked, linked_source, score)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id_source, source, id_linked, linked_source) DO UPDATE SET score = EXCLUDED.score;
            """, (prod_id, source, match['id'], other_source, match['score']))
    conn.commit()
    cur.close()

if __name__ == '__main__':
    conn = get_db_connection()
    if conn is None:
        print("Connexion à la base impossible.")
    else:
        create_ingredient_link_table(conn)
        fill_ingredient_links(conn)
        conn.close()
