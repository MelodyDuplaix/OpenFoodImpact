import psycopg2
import os
import pandas as pd
import concurrent.futures
import random
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

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
    df_exact = pd.read_sql(SQL_ANALYSE, conn)
    print(df_exact.to_string(index=False))
    # --- PARTIE 2 : fuzzy + vector search ---
    print("\nAnalyse des produits similaires (fuzzy + vector) entre sources (parallélisé) :")
    cur.execute("SELECT DISTINCT source FROM product_vector;")
    sources = [row[0] for row in cur.fetchall()]
    seuil_fuzzy = 0.65
    sample_size = 1000
    results = []

    def count_similar_names_both_ways(s1, s2):
        # Analyse s1 -> s2
        local_conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB', 'postgres'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        local_cur = local_conn.cursor()
        # Echantillon source1
        local_cur.execute("SELECT name FROM product_vector WHERE source = %s;", (s1,))
        names1 = [row[0] for row in local_cur.fetchall()]
        if len(names1) > sample_size:
            names1 = random.sample(names1, sample_size)
        count_similar_1 = 0
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
                count_similar_1 += 1
        total1 = len(names1)
        percent1 = 100 * count_similar_1 / total1 if total1 else 0
        # Echantillon source2
        local_cur.execute("SELECT name FROM product_vector WHERE source = %s;", (s2,))
        names2 = [row[0] for row in local_cur.fetchall()]
        if len(names2) > sample_size:
            names2 = random.sample(names2, sample_size)
        count_similar_2 = 0
        for name in names2:
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
            """, (name, s2, s1, seuil_fuzzy))
            if local_cur.fetchone():
                count_similar_2 += 1
        total2 = len(names2)
        percent2 = 100 * count_similar_2 / total2 if total2 else 0
        local_cur.close()
        local_conn.close()
        return {
            'source1': s1,
            'source2': s2,
            'similar_count_source1': count_similar_1,
            'sampled_total_source1': total1,
            'percent_source1': round(percent1, 2),
            'similar_count_source2': count_similar_2,
            'sampled_total_source2': total2,
            'percent_source2': round(percent2, 2)
        }

    pairs = [(s1, s2) for i, s1 in enumerate(sources) for s2 in sources[i+1:]]
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(pairs))) as executor:
        for res in executor.map(lambda args: count_similar_names_both_ways(*args), pairs):
            results.append(res)
    df_fuzzy = pd.DataFrame(results)
    print("\nRésultats fuzzy/vector sur un échantillon (parallélisé, rapide, 2 sens) :")
    print(df_fuzzy.to_string(index=False))
    cur.close()
    conn.close()

    pdf_path = "docs/comparaison_similarite_sources.pdf"
    with PdfPages(pdf_path) as pdf:
        # Page 1 : Tableaux comparatifs
        fig, axes = plt.subplots(2, 1, figsize=(12, 10))
        axes[0].axis('off')
        axes[0].set_title('Correspondance exacte entre sources')
        table0 = axes[0].table(cellText=df_exact.values, colLabels=df_exact.columns, loc='center')
        table0.auto_set_font_size(False)
        table0.set_fontsize(8)
        table0.scale(1, 1.5)
        axes[1].axis('off')
        axes[1].set_title('Similarité fuzzy + vector (échantillon, 2 sens) entre sources')
        table1 = axes[1].table(cellText=df_fuzzy.values, colLabels=df_fuzzy.columns, loc='center')
        table1.auto_set_font_size(False)
        table1.set_fontsize(8)
        table1.scale(1, 1.5)
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)
        # Page 2 : Barplot comparatif
        df_merge = pd.merge(
            df_exact[['source1', 'source2', 'percent_source1', 'percent_source2']],
            df_fuzzy[['source1', 'source2', 'percent_source1', 'percent_source2']],
            on=['source1', 'source2'],
            suffixes=('_exact', '_fuzzy')
        )
        fig2, ax2 = plt.subplots(figsize=(14, 7))
        bar_width = 0.2
        x = range(len(df_merge))
        ax2.bar(x, df_merge['percent_source1_exact'], width=bar_width, label='Exact (source1)')
        ax2.bar([i + bar_width for i in x], df_merge['percent_source1_fuzzy'], width=bar_width, label='Fuzzy+Vector (source1)')
        ax2.bar([i + 2*bar_width for i in x], df_merge['percent_source2_exact'], width=bar_width, label='Exact (source2)', alpha=0.5)
        ax2.bar([i + 3*bar_width for i in x], df_merge['percent_source2_fuzzy'], width=bar_width, label='Fuzzy+Vector (source2)', alpha=0.5)
        ax2.set_xticks([i + bar_width*1.5 for i in x])
        ax2.set_xticklabels([f"{a} vs {b}" for a, b in zip(df_merge['source1'], df_merge['source2'])], rotation=45, ha='right')
        ax2.set_ylabel('Pourcentage de similarité (%)')
        ax2.set_title('Comparaison du pourcentage de similarité entre sources (2 sens)')
        ax2.legend()
        plt.tight_layout()
        pdf.savefig(fig2)
        plt.close(fig2)
    print(f"\nPDF généré : {pdf_path}")

if __name__ == '__main__':
    analyse_product_vector_overlap()
