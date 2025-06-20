from processing.utils import get_db_connection
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
    """
    Analyse le chevauchement des produits entre sources et génère un rapport PDF.

    Args:
        None
    Returns:
        None: Génère un fichier PDF et affiche des informations dans la console.
    """
    conn = get_db_connection()
    if conn is None:
        print("Connexion à la base impossible.")
        return
    cur = conn.cursor()
    print("Création de l'index (si besoin)...")
    cur.execute(SQL_INDEX)
    print("Analyse des produits strictement identiques entre sources :")
    # on récupère les correspondances exactes entre sources
    df_exact = pd.read_sql(sql=SQL_ANALYSE, conn) # type: ignore
    print(df_exact.to_string(index=False))
    print("\nAnalyse des produits similaires (fuzzy + vector) entre sources:")
    cur.execute("SELECT DISTINCT source FROM product_vector;")
    sources = [row[0] for row in cur.fetchall()]
    seuil_fuzzy = 0.65
    sample_size = 1000
    results = []

    def count_similar_names_both_ways(s1, s2):
        """
        Compte les noms similaires entre deux sources (s1, s2) dans les deux directions.

        Args:
            s1 (str): Première source.
            s2 (str): Deuxième source.
        Returns:
            dict: Résultats de la comparaison (comptes, totaux, pourcentages).
        """
        local_conn = get_db_connection()
        if local_conn is None:
            return {
                'source1': s1,
                'source2': s2,
                'similar_count_source1': 0,
                'sampled_total_source1': 0,
                'percent_source1': 0.0,
                'similar_count_source2': 0,
                'sampled_total_source2': 0,
                'percent_source2': 0.0
            }
        local_cur = local_conn.cursor()
        local_cur.execute("SELECT name FROM product_vector WHERE source = %s;", (s1,))
        names1 = [row[0] for row in local_cur.fetchall()]
        # on prend un échantillon si trop de noms
        if len(names1) > sample_size:
            names1 = random.sample(names1, sample_size)
        count_similar_1 = 0
        for name in names1:
            # on cherche si ce nom a un match fuzzy+vector dans l'autre source
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
        local_cur.execute("SELECT name FROM product_vector WHERE source = %s;", (s2,))
        names2 = [row[0] for row in local_cur.fetchall()]
        # on prend un échantillon si trop de noms
        if len(names2) > sample_size:
            names2 = random.sample(names2, sample_size)
        count_similar_2 = 0
        for name in names2:
            # on cherche si ce nom a un match fuzzy+vector dans l'autre sens
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
    # on lance les comparaisons en parallèle pour accélérer le calcul
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(pairs))) as executor:
        for res in executor.map(lambda args: count_similar_names_both_ways(*args), pairs):
            results.append(res)
    df_fuzzy = pd.DataFrame(results)
    print("\nRésultats fuzzy/vector sur un échantillon:")
    print(df_fuzzy.to_string(index=False))
    cur.close()
    conn.close()
    seuils = [0.5, 0.6, 0.7, 0.8]
    sample_pairs = []
    for seuil in seuils:
        print(f"\nExtraction d'un échantillon de paires matchées pour seuil global_score > {seuil}...")
        for s1 in sources:
            for s2 in sources:
                if s1 == s2:
                    continue
                cur = get_db_connection()
                if cur is None:
                    continue
                cur = cur.cursor()
                cur.execute("SELECT name FROM product_vector WHERE source = %s;", (s1,))
                names1 = [row[0] for row in cur.fetchall()]
                # on prend un échantillon de 50 noms pour chaque source
                if len(names1) > 50:
                    names1 = random.sample(names1, 50)
                for name in names1:
                    # on cherche le meilleur match fuzzy+vector dans l'autre source
                    cur.execute(f"""
                        WITH reference AS (
                            SELECT name, name_vector FROM product_vector WHERE name = %s AND source = %s
                        )
                        SELECT pv.name, pv.source, pv.code_source,
                               (0.4 * (1 - (pv.name_vector <=> r.name_vector)) + 0.6 * similarity(pv.name, r.name)) AS global_score,
                               1 - (pv.name_vector <=> r.name_vector) AS vector_similarity,
                               similarity(pv.name, r.name) AS fuzzy_similarity
                        FROM product_vector pv
                        CROSS JOIN reference r
                        WHERE pv.source = %s
                        ORDER BY global_score DESC
                        LIMIT 1;
                    """, (name, s1, s2))
                    match = cur.fetchone()
                    if match and match[3] > seuil:
                        sample_pairs.append({
                            'source1': s1,
                            'name1': name,
                            'source2': match[1],
                            'name2': match[0],
                            'code_source2': match[2],
                            'global_score': round(match[3], 3),
                            'vector_similarity': round(match[4], 3),
                            'fuzzy_similarity': round(match[5], 3),
                            'seuil': seuil
                        })
                cur.close()
    df_sample = pd.DataFrame(sample_pairs)
    pdf_path = "docs/comparaison_similarite_sources.pdf"
    with PdfPages(pdf_path) as pdf:
        fig, axes = plt.subplots(2, 1, figsize=(11.69, 8.27)) 
        axes[0].axis('off')
        axes[0].set_title('Correspondance exacte entre sources')
        table0 = axes[0].table(cellText=df_exact.values, colLabels=df_exact.columns, loc='center')
        table0.auto_set_font_size(False)
        table0.set_fontsize(8)
        table0.scale(1, 1.5)
        axes[1].axis('off')
        axes[1].set_title('Similarité fuzzy + vector (échantillon, 2 sens, seuil=0.65) entre sources')
        table1 = axes[1].table(cellText=df_fuzzy.values, colLabels=df_fuzzy.columns, loc='center')
        table1.auto_set_font_size(False)
        table1.set_fontsize(8)
        table1.scale(1, 1.5)
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)
        # on fusionne les résultats exacts et fuzzy pour la comparaison graphique
        df_merge = pd.merge(
            df_exact[['source1', 'source2', 'percent_source1', 'percent_source2']],
            df_fuzzy[['source1', 'source2', 'percent_source1', 'percent_source2']],
            on=['source1', 'source2'],
            suffixes=('_exact', '_fuzzy065')
        )
        fuzzy_seuils = [0.5, 0.6, 0.7, 0.8]
        taux_fuzzy = {}
        for seuil in fuzzy_seuils:
            taux = []
            for i, row in df_merge.iterrows():
                s1, s2 = row['source1'], row['source2']
                n = len(df_sample[(df_sample['source1']==s1)&(df_sample['source2']==s2)&(df_sample['seuil']==seuil)])
                total = len(df_sample[(df_sample['source1']==s1)&(df_sample['source2']==s2)&(df_sample['seuil']==seuil)])
                taux.append(100*n/50 if total else 0)
            taux_fuzzy[seuil] = taux
        fig2, ax2 = plt.subplots(figsize=(11.69, 8.27))
        bar_width = 0.13
        x = range(len(df_merge))
        # on trace les barres pour chaque type de matching
        ax2.bar([i-2*bar_width for i in x], df_merge['percent_source1_exact'], width=bar_width, label='Exact (source1)')
        ax2.bar([i-bar_width for i in x], df_merge['percent_source2_exact'], width=bar_width, label='Exact (source2)', alpha=0.5)
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        for idx, seuil in enumerate(fuzzy_seuils):
            ax2.bar([i+(idx)*bar_width for i in x], taux_fuzzy[seuil], width=bar_width, label=f'Fuzzy+Vector (seuil={seuil})', color=colors[idx], alpha=0.7)
        ax2.set_xticks([i for i in x])
        ax2.set_xticklabels([f"{a} vs {b}" for a, b in zip(df_merge['source1'], df_merge['source2'])], rotation=45, ha='right')
        ax2.set_ylabel('Pourcentage de similarité (%)')
        ax2.set_title('Comparaison du pourcentage de similarité entre sources (matching exact et fuzzy+vector, multi-seuils)')
        ax2.legend()
        plt.tight_layout()
        pdf.savefig(fig2)
        plt.close(fig2)
        for seuil in seuils:
            df_seuil = df_sample[df_sample['seuil'] == seuil]
            if not df_seuil.empty:
                fig3, ax3 = plt.subplots(figsize=(11.69, min(8.27, 0.5*len(df_seuil)+2)))
                ax3.axis('off')
                ax3.set_title(f'Echantillon de paires matchées (global_score > {seuil})')
                # on affiche un échantillon de paires matchées pour chaque seuil
                table3 = ax3.table(cellText=df_seuil.sample(n=min(15, len(df_seuil)), random_state=42).values.tolist(), colLabels=list(df_seuil.columns), loc='center')
                table3.auto_set_font_size(False)
                table3.set_fontsize(8)
                table3.scale(1, 1.5)
                plt.tight_layout()
                pdf.savefig(fig3)
                plt.close(fig3)
        cur2 = get_db_connection()
        if cur2 is None:
            return
        cur2 = cur2.cursor()
        def fetch_count_or_zero():
            res = cur2.fetchone()
            return res[0] if res and res[0] is not None else 0
        # on compte les produits présents dans toutes les sources (matching exact)
        cur2.execute("""
            SELECT COUNT(*) FROM (
                SELECT name
                FROM product_vector
                GROUP BY name
                HAVING COUNT(DISTINCT source) = (SELECT COUNT(DISTINCT source) FROM product_vector)
            ) t;
        """)
        nb_exact_all = fetch_count_or_zero()
        # on compte les produits liés à toutes les sources via ingredient_link (fuzzy+vector)
        cur2.execute("""
            SELECT COUNT(*) FROM (
                SELECT id_source, source, COUNT(DISTINCT linked_source) AS nb_sources_liees
                FROM ingredient_link
                GROUP BY id_source, source
                HAVING COUNT(DISTINCT linked_source) = (
                    SELECT COUNT(DISTINCT source) - 1 FROM product_vector
                )
            ) t;
        """)
        nb_fuzzy_all = fetch_count_or_zero()
        # on compte les produits présents dans toutes les sources sauf greenpeace (matching exact)
        cur2.execute("""
            SELECT COUNT(*) FROM (
                SELECT name
                FROM product_vector
                WHERE source != 'greenpeace_season'
                GROUP BY name
                HAVING COUNT(DISTINCT source) = (
                    SELECT COUNT(DISTINCT source) FROM product_vector WHERE source != 'greenpeace'
                )
            ) t;
        """)
        nb_exact_no_gp = fetch_count_or_zero()
        # on compte les produits liés à toutes les sources sauf greenpeace (fuzzy+vector)
        cur2.execute("""
            SELECT COUNT(*) FROM (
                SELECT id_source, source, COUNT(DISTINCT linked_source) AS nb_sources_liees
                FROM ingredient_link
                WHERE source != 'greenpeace' AND linked_source != 'greenpeace_season'
                GROUP BY id_source, source
                HAVING COUNT(DISTINCT linked_source) = (
                    SELECT COUNT(DISTINCT source) - 1 FROM product_vector WHERE source != 'greenpeace'
                )
            ) t;
        """)
        nb_fuzzy_no_gp = fetch_count_or_zero()

        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.axis('off')
        ax.set_title('Synthèse matching exact vs fuzzy+vector (toutes sources et hors greenpeace)', fontsize=14)
        table_data = [
            ["Type de matching", "Toutes sources", "Hors greenpeace"],
            ["Matching exact (noms identiques)", nb_exact_all, nb_exact_no_gp],
            ["Fuzzy+vector (liens)", nb_fuzzy_all, nb_fuzzy_no_gp],
        ]
        table = ax.table(cellText=table_data, loc='center', cellLoc='center', colLabels=None, colColours=["#f0f0f0"]*3)
        table.auto_set_font_size(False)
        table.set_fontsize(12)
        table.scale(1, 2)
        percent_gain = 0
        if nb_exact_all > 0:
            percent_gain = round(100 * (nb_fuzzy_all - nb_exact_all) / nb_exact_all, 1)
        phrase = f"L'utilisation du fuzzy+vector permet d'augmenter le nombre de correspondances multi-sources de {percent_gain}% par rapport au matching exact."
        ax.text(0.5, -0.15, phrase, ha='center', va='center', fontsize=11, transform=ax.transAxes)
        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)
        cur2.close()

    print(f"\nPDF généré : {pdf_path}")
    print("\nINTERPRÉTATION :")
    print(f"- Matching exact (toutes sources) : {nb_exact_all} produits présents dans toutes les sources avec le même nom.")
    print(f"- Fuzzy+vector (toutes sources) : {nb_fuzzy_all} produits ont un lien vers toutes les autres sources (similaires, pas forcément nom identique).")
    print(f"- Matching exact (hors greenpeace) : {nb_exact_no_gp} produits présents dans toutes les sources hors greenpeace avec le même nom.")
    print(f"- Fuzzy+vector (hors greenpeace) : {nb_fuzzy_no_gp} produits ont un lien vers toutes les autres sources hors greenpeace.")
    print("\nCela permet de voir l'apport du fuzzy+vector par rapport au matching exact, et l'effet de la source greenpeace sur la couverture.")

if __name__ == '__main__':
    analyse_product_vector_overlap()