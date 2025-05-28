from urllib import response
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import time
import pymongo
from urllib.parse import urljoin

recipes_types = ["entree", "plat-principal", "dessert", "boissons"]
base_url = "https://www.marmiton.org/recettes/index/categorie/"

def scrapes_recipe_list():
    """
    Scrapes the recipe list from Marmiton website.

    Returns:
        list: A list of dictionaries containing recipe titles and links.
    """
    recipes = []
    for recipe_type in recipes_types:
        print(f"scraping {recipe_type} recipes")
        page = 1
        while True:
            url = f"{base_url}{recipe_type}/" if page == 1 else f"{base_url}{recipe_type}/{page}"
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except Exception as e:
                print(f"Request failed for {url}: {e}")
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
                link = urljoin(base_url, link_element["href"]) # type: ignore
                recipes.append({"title": title, "link": link})
            page += 1
            time.sleep(0.05)
    return recipes

def extract_schemaorg_recipe(url):
    """
    Extracts the recipe data from a given URL using schema.org JSON-LD format.

    Args:
        url (str): The URL of the recipe page.

    Returns:
        dict: A dictionary containing the recipe data if found, otherwise None.
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string) # type: ignore
            if isinstance(data, list):
                for entry in data:
                    if entry.get("@type") == "Recipe":
                        return entry
            elif isinstance(data, dict) and data.get("@type") == "Recipe":
                return data
        except Exception:
            continue
    return None

def insert_recipes(recipes):
    """Inserts recipes into MongoDB."""
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["OpenFoodImpact"]
    collection = db["recipes"]
    try:
        collection.insert_many(recipes)
        print("Recipes inserted successfully!")
    except Exception as e:
        print(f"Error inserting recipes: {e}")
    finally:
        client.close()

def remove_objectid(data):
    if isinstance(data, dict):
        return {k: remove_objectid(v) for k, v in data.items() if k != "_id"}
    elif isinstance(data, list):
        return [remove_objectid(item) for item in data]
    else:
        return data

def extract_all_recipes():
    """
    Extracts all recipes from Marmiton website and inserts them into MongoDB.

    Returns:
        list: A list of dictionaries containing recipe titles, links, and details.
    """
    start_time = time.time()
    recipes = scrapes_recipe_list()
    print(f"found {len(recipes)} recipes, now extracting details")
    for recipe in recipes:
        recipe_data = extract_schemaorg_recipe(recipe["link"])
        if recipe_data:
            recipe.update(remove_objectid(recipe_data))
        else:
            print(f"Failed to extract recipe data for {recipe['title']}")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time to extract all recipes: {total_time:.2f} seconds")
    recipes = remove_objectid(recipes)
    insert_recipes(recipes)
    return recipes

if __name__ == "__main__":
    print("scraping marmiton recipes, this may take a while...")
    recipes = extract_all_recipes()
    print(f"scraped {len(recipes)} recipes")   
    print("saving recipes to marmiton_recipes.json")
    recipes = remove_objectid(recipes)  # Remove MongoDB ObjectId fields
    with open("data/marmiton_recipes.json", "w", encoding="utf-8") as f:
        json.dump(recipes, f, ensure_ascii=False, indent=4)
    print("recipes saved successfully")
    print("exemplary recipe:")
    print(recipes[0])
