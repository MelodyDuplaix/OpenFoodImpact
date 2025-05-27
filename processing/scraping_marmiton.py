from urllib import response
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json

recipes_types = ["entree", "plat-principal", "dessert", "boissons"]
base_url = "https://www.marmiton.org/recettes/index/categorie/"

def scrapes_recipe_list():
    for recipe_type in recipes_types:
        print(f"scraping {recipe_type} recipes")
        url = f"{base_url}{recipe_type}/"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        recipes = []
        for recipe in soup.select(".type-Recipe"):
            title = recipe.select_one(".mrtn-card__title").get_text(strip=True)
            link = recipe.select_one("a")["href"]
            recipes.append({"title": title, "link": link})
        next = True
        page = 1
        while next and len(recipes) < 5000:
            url = f"{base_url}{recipe_type}/{page}"
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            if not soup.select(".type-Recipe"):
                next = False
                break
            for recipe in soup.select(".type-Recipe"):
                title = recipe.select_one(".mrtn-card__title").get_text(strip=True)
                link = recipe.select_one("a")["href"]
                recipes.append({"title": title, "link": link})
    return recipes

def extract_schemaorg_recipe(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                for entry in data:
                    if entry.get("@type") == "Recipe":
                        return entry
            elif isinstance(data, dict) and data.get("@type") == "Recipe":
                return data
        except Exception:
            continue
    return None

def extract_all_recipes(recipes_types):
    recipes = scrapes_recipe_list()
    for recipe in recipes:
        recipe_data = extract_schemaorg_recipe(recipe["link"])
        if recipe_data:
            recipe["details"] = recipe_data
        else:
            print(f"Failed to extract recipe data for {recipe['title']}")
    return recipes

if __name__ == "__main__":
    print("scraping marmiton recipes, this may take a while...")
    recipes = extract_all_recipes(recipes_types)
    print(f"scraped {len(recipes)} recipes")   
    print("saving recipes to marmiton_recipes.json")
    with open("data/marmiton_recipes.json", "w", encoding="utf-8") as f:
        json.dump(recipes, f, ensure_ascii=False, indent=4)
    print("recipes saved successfully")
    print("exemplary recipe:")
    print(recipes[0])
                
