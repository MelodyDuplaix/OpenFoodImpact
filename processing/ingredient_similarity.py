
import logging
logger = logging.getLogger(__name__)

def find_similar_ingredients(name, source, conn, min_score=0.65):
    """
    Trouve les produits similaires à un ingrédient donné dans d'autres sources.

    Args:
        name (str): Nom de l'ingrédient de référence.
        source (str): Source de l'ingrédient de référence.
        conn (psycopg2.extensions.connection): Connexion à la base de données PostgreSQL.
        min_score (float, optional): Score de similarité global minimal. Défaut à 0.65.
    Returns:
        dict: Clés = autres sources, Valeurs = {'id', 'name', 'score'} du meilleur match.
        Utilise un matching exact pour greenpeace vs (marmiton/agribalyse), sinon fuzzy+vector.
    """
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT source FROM product_vector;")
    all_sources = [row[0] for row in cur.fetchall()]
    results = {}
    for other_source in all_sources:
        if other_source == source:
            continue
        if (source == 'greenpeace' and other_source in ['marmiton', 'agribalyse']) or \
           (other_source == 'greenpeace' and source in ['marmiton', 'agribalyse']):
            # Matching exact
            cur.execute("""
                SELECT id, name FROM product_vector
                WHERE name = %s AND source = %s
                LIMIT 1;
            """, (name, other_source))
            match = cur.fetchone()
            if match:
                results[other_source] = {'id': match[0], 'name': match[1], 'score': 1.0}
        else:
            # Fuzzy + vector
            cur.execute("""
                WITH reference AS (
                    SELECT name, name_vector FROM product_vector WHERE name = %s AND source = %s
                )
                SELECT pv.id, pv.name, (0.4 * (1 - (pv.name_vector <=> r.name_vector)) + 0.6 * similarity(pv.name, r.name)) AS global_score
                FROM product_vector pv
                CROSS JOIN reference r
                WHERE pv.source = %s
                ORDER BY global_score DESC
                LIMIT 1;
            """, (name, source, other_source))
            match = cur.fetchone()
            if match and match[2] >= min_score:
                results[other_source] = {'id': match[0], 'name': match[1], 'score': match[2]}
        logger.debug(f"Found {len(results)} similar ingredients for {name} in source {source} against {other_source}")
    cur.close()
    return results
