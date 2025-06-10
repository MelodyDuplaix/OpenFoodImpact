from bs4 import BeautifulSoup
import requests
import json
import time
from pymongo import MongoClient
from urllib.parse import urljoin
from dotenv import load_dotenv
import os
import logging
load_dotenv()

recipes_types = ["entree", "plat-principal", "dessert", "boissons"]
base_url = "https://www.marmiton.org/recettes/index/categorie/"

def scrapes_recipe_list():
    """
    Scrape la liste des recettes depuis Marmiton.
    Retourne une liste de dictionnaires avec titres et liens.
    """
    recipes = []
    for recipe_type in recipes_types:
        logging.info(f"Scraping {recipe_type} recipes")
        page = 1
        while True:
            url = f"{base_url}{recipe_type}/" if page == 1 else f"{base_url}{recipe_type}/{page}"
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except Exception as e:
                logging.warning(f"Request failed for {url}: {e}")
                break
            soup = BeautifulSoup(response.content, "html.parser")
            recipe_cards = soup.select(".type-Recipe")
            if not recipe_cards or len(recipes) >= 5000:
                break
            for recipe in recipe_cards:
                title_element = recipe.select_one(".mrtn-card__title")
                link_element = recipe.select_one("a")
                if not title_element or not link_element:
                    continue
                if "href" not in link_element.attrs:
                    continue
                title = title_element.get_text(strip=True)
                link = urljoin(base_url, str(link_element["href"]))
                recipes.append({"title": title, "link": link})
            page += 1
            time.sleep(0.05)
    return recipes

def extract_schemaorg_recipe(url):
    """
    Extrait les données recette d'une URL Marmiton via schema.org JSON-LD.
    Retourne un dictionnaire recette ou None si échec.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                script_content = script.get_text()
                if not script_content:
                    continue
                data = json.loads(script_content)
                if isinstance(data, list):
                    for entry in data:
                        if entry.get("@type") == "Recipe":
                            return entry
                elif isinstance(data, dict) and data.get("@type") == "Recipe":
                    return data
            except Exception:
                continue
    except Exception as e:
        logging.warning(f"Failed to extract schema.org recipe from {url}: {e}")
    return None

def insert_recipes(recipes):
    """
    Insère les recettes dans MongoDB.
    """
    try:
        client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
        db = client["OpenFoodImpact"]
        collection = db["recipes"]
        if recipes:
            collection.insert_many(recipes, ordered=False)
            logging.info(f"{len(recipes)} recipes inserted successfully into MongoDB!")
        else:
            logging.warning("No recipes to insert into MongoDB.")
    except Exception as e:
        logging.error(f"Error inserting recipes into MongoDB: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass

def remove_objectid(data):
    """
    Retire les champs _id des objets MongoDB (pour export propre).
    """
    if isinstance(data, dict):
        return {k: remove_objectid(v) for k, v in data.items() if k != "_id"}
    elif isinstance(data, list):
        return [remove_objectid(item) for item in data]
    else:
        return data

def extract_all_recipes():
    """
    Extrait toutes les recettes Marmiton et les insère dans MongoDB.
    Retourne une liste de recettes (titres, liens, détails).
    """
    start_time = time.time()
    try:
        recipes = scrapes_recipe_list()
        logging.info(f"Found {len(recipes)} recipes, now extracting details")
        for recipe in recipes:
            try:
                recipe_data = extract_schemaorg_recipe(recipe["link"])
                if recipe_data:
                    recipe.update(remove_objectid(recipe_data))
                else:
                    logging.warning(f"Failed to extract recipe data for {recipe['title']}")
            except Exception as e:
                logging.warning(f"Erreur extraction détails recette : {e}")
        recipes = remove_objectid(recipes)
        insert_recipes(recipes)
        total_time = time.time() - start_time
        logging.info(f"Total time to extract all recipes: {total_time:.2f} seconds")
        return recipes
    except Exception as e:
        logging.error(f"Error in extract_all_recipes: {e}")
        return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Scraping Marmiton recipes, this may take a while...")
    recipes = extract_all_recipes()
    logging.info(f"Scraped {len(recipes)} recipes")
