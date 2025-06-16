import psycopg2
from processing.ingredient_similarity import find_similar_ingredients

def create_ingredient_link_table(conn):
    """
    Crée la table ingredient_link si elle n'existe pas déjà.
    
    Args:
        conn (psycopg2.extensions.connection): Connexion à la base de données PostgreSQL.
        
    Returns:
        None
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingredient_link (
            id SERIAL PRIMARY KEY,
            id_source INTEGER REFERENCES product_vector(id),
            source TEXT,
            id_linked INTEGER REFERENCES product_vector(id),
            linked_source TEXT,
            score FLOAT
        );
    """)
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
    Remplit la table ingredient_link avec les liens entre ingrédients similaires
    
    Args:
        conn (psycopg2.extensions.connection): Connexion à la base de données PostgreSQL.
        
    Returns:
        None
    """
    cur = conn.cursor()
    cur.execute("SELECT id, name, source FROM product_vector;")
    all_products = cur.fetchall()
    for prod_id, name, source in all_products:
        similars = find_similar_ingredients(name, source, conn)
        for other_source, match in similars.items():
            cur.execute("""
                INSERT INTO ingredient_link (id_source, source, id_linked, linked_source, score)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (prod_id, source, match['id'], other_source, match['score']))
    conn.commit()
    cur.close()

if __name__ == '__main__':
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )
    create_ingredient_link_table(conn)
    fill_ingredient_links(conn)
    conn.close()
