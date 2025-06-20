from bs4 import BeautifulSoup
import requests
from sentence_transformers import SentenceTransformer
from .utils import get_db_connection, normalize_name, vectorize_name, safe_execute

greenpeace_url = "https://www.greenpeace.fr/guetteur/calendrier/"

def scrape_greenpeace_calendar():
    """
    Récupère le calendrier des fruits et légumes de saison depuis Greenpeace.

    Args:
        None
    Returns:
        dict: Dictionnaire {mois: [produits]}, ou {} en cas d'erreur.
    """
    try:
        response = requests.get(greenpeace_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        calendrier = {}
        for mois_section in soup.select(".month"):
            # on boucle sur les mois qui sont des sections avec la classe "month"
            # le nom du mois est un lien <a> dans la section
            month = mois_section.select_one("a")
            if not month:
                continue
            month = month["name"]
            # dans le mois il y a une section avec la classe "list-legumes" qui contient les légumes de saison sous forme de liste
            legumes = mois_section.select(".list-legumes")
            legumes_list = [legume.get_text(strip=True) for legume in legumes[0].find_all("li")] if legumes else []
            calendrier[month] = legumes_list
        return calendrier
    except Exception as e:
        print(f"Erreur scraping Greenpeace : {e}")
        return {}

def vectorize_product_name(name):
    """
    Vectorise un nom de produit en utilisant un modèle SentenceTransformer.

    Args:
        name (str): Nom du produit à vectoriser.
    Returns:
        list: Liste de flottants représentant le vecteur du nom.
    """
    if not hasattr(vectorize_product_name, "model"):
        vectorize_product_name.model = SentenceTransformer('all-MiniLM-L6-v2')
    embedding = vectorize_product_name.model.encode([name])[0]
    return embedding.tolist()

def insert_season_data_to_db(season_data):
    """
    Insère les données de saisonnalité Greenpeace dans product_vector et greenpeace_season.

    Args:
        season_data (dict): Dictionnaire {mois: [produits]} scrapé.
    Returns:
        None: Modifie la base de données.
    """
    conn = get_db_connection()
    if conn is None:
        print("Connexion à la base impossible.")
        return
    cur = conn.cursor()
    for month, items in season_data.items():
        for name in items:
                # on normalise et vectorise le nom pour pouvoir l'ajouter à product_vector
                name_normalized = normalize_name(name)
                name_vector = vectorize_name(name_normalized)
                safe_execute(cur, """
                    INSERT INTO product_vector (name, name_vector, source)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id;
                """, (name_normalized, name_vector, 'greenpeace'))
                result = cur.fetchone()
                # on récupère l'id du produit pour l'insérer dans greenpeace_season
                # soit directement si l'insert a réussi, soit en le cherchant dans product_vector si le produit existait déjà
                if result:
                    product_vector_id = result[0]
                else:
                    safe_execute(cur, "SELECT id FROM product_vector WHERE name = %s AND source = %s;", (name_normalized, 'greenpeace'))
                    fetch = cur.fetchone()
                    if not fetch:
                        continue
                    product_vector_id = fetch[0]
                safe_execute(cur, """
                    INSERT INTO greenpeace_season (product_vector_id, month)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """, (product_vector_id, month))            
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    print("Scraping Greenpeace calendar...")
    calendar_data = scrape_greenpeace_calendar()
    print("Inserting data into database...")
    insert_season_data_to_db(calendar_data)
    print("Scraping and insertion completed.")