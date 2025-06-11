import psycopg2
import os
import pandas as pd
import concurrent.futures
import random

SQL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_product_vector_name_source ON product_vector (name, source);
"""

SQL_ANALYSE = """
WITH names_by_source AS (
  SELECT name, source
  FROM product_vector
  GROUP BY name, source
),
pairs AS (
  SELECT s1.source AS source1, s2.source AS source2
  FROM (SELECT DISTINCT source FROM product_vector) s1
  JOIN (SELECT DISTINCT source FROM product_vector) s2
    ON s1.source < s2.source
),
common_names AS (
  SELECT
    p.source1,
    p.source2,
    COUNT(*) AS common_count
  FROM pairs p
  JOIN names_by_source n1 ON n1.source = p.source1
  JOIN names_by_source n2 ON n2.source = p.source2 AND n1.name = n2.name
  GROUP BY p.source1, p.source2
),
total_names AS (
  SELECT
    source AS source1,
    COUNT(DISTINCT name) AS total1
  FROM product_vector
  GROUP BY source
)
SELECT
  c.source1,
  c.source2,
  c.common_count,
  t1.total1 AS total_source1,
  t2.total1 AS total_source2,
  ROUND(100.0 * c.common_count / t1.total1, 2) AS percent_source1,
  ROUND(100.0 * c.common_count / t2.total1, 2) AS percent_source2
FROM common_names c
JOIN total_names t1 ON c.source1 = t1.source1
JOIN total_names t2 ON c.source2 = t2.source1
ORDER BY percent_source1 DESC, percent_source2 DESC;
"""

def analyse_product_vector_overlap():
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'postgres'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432')
    )
    cur = conn.cursor()
    print("Création de l'index (si besoin)...")
    cur.execute(SQL_INDEX)
    conn.commit()
    print("Analyse des produits strictement identiques entre sources :")
    df = pd.read_sql(SQL_ANALYSE, conn)
    print(df.to_string(index=False))
    # --- PARTIE 2 : fuzzy + vector search ---
    print("\nAnalyse des produits similaires (fuzzy + vector) entre sources (parallélisé) :")
    cur.execute("SELECT DISTINCT source FROM product_vector;")
    sources = [row[0] for row in cur.fetchall()]
    seuil_fuzzy = 0.65
    sample_size = 1000  # Taille de l'échantillon par source
    results = []

    def count_similar_names(s1, s2):
        local_conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB', 'postgres'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        local_cur = local_conn.cursor()
        local_cur.execute("SELECT name FROM product_vector WHERE source = %s;", (s1,))
        names1 = [row[0] for row in local_cur.fetchall()]
        if len(names1) > sample_size:
            names1 = random.sample(names1, sample_size)
        count_similar = 0
        for name in names1:
            local_cur.execute(f"""
                WITH reference AS (
                    SELECT name, name_vector FROM product_vector WHERE name = %s AND source = %s
                )
                SELECT 1
                FROM product_vector pv
                CROSS JOIN reference r
                WHERE pv.source = %s
                  AND (0.4 * (1 - (pv.name_vector <=> r.name_vector)) + 0.6 * similarity(pv.name, r.name)) > %s
                LIMIT 1;
            """, (name, s1, s2, seuil_fuzzy))
            if local_cur.fetchone():
                count_similar += 1
        total1 = len(names1)
        local_cur.close()
        local_conn.close()
        percent1 = 100 * count_similar / total1 if total1 else 0
        return {
            'source1': s1,
            'source2': s2,
            'similar_count': count_similar,
            'sampled_total_source1': total1,
            'percent_source1': round(percent1, 2)
        }

    pairs = [(s1, s2) for i, s1 in enumerate(sources) for s2 in sources[i+1:]]
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(pairs))) as executor:
        for res in executor.map(lambda args: count_similar_names(*args), pairs):
            results.append(res)
    print("\nRésultats fuzzy/vector sur un échantillon (parallélisé, rapide) :")
    print(pd.DataFrame(results).to_string(index=False))
    cur.close()
    conn.close()

if __name__ == '__main__':
    analyse_product_vector_overlap()
