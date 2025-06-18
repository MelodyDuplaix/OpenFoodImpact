
import os
import re
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def convert_iso8601_to_minutes(duration):
    """
    Convertit une durée ISO 8601 (PTxHyM) ou un entier en minutes.

    Args:
        duration (str or int): Durée à convertir.
    Returns:
        int: Durée en minutes, ou 0 si la conversion échoue.
    """
    if isinstance(duration, int):
        return duration
    if not isinstance(duration, str):
        return 0
    match = re.match(r'PT(\d+H)?(\d+M)?', duration)
    if not match:
        return 0
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    return hours * 60 + minutes

def convert_recipe_times():
    """
    Convertit les champs de temps (prepTime, cookTime, totalTime) des recettes MongoDB en minutes.

    Args:
        None
    Returns:
        None: Modifie les documents dans la collection MongoDB.
    """
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"))
    db = client["OpenFoodImpact"]
    collection = db["recipes"]

    for recipe in collection.find({}):
        updates = {}
        if 'prepTime' in recipe:
            updates['prepTime'] = convert_iso8601_to_minutes(recipe['prepTime'])
        if 'cookTime' in recipe:
            updates['cookTime'] = convert_iso8601_to_minutes(recipe['cookTime'])
        if 'totalTime' in recipe:
            updates['totalTime'] = convert_iso8601_to_minutes(recipe['totalTime'])
        
        if updates:
            collection.update_one({'_id': recipe['_id']}, {'$set': updates})

    client.close()
    print("Conversion des temps de recettes terminée.")

if __name__ == "__main__":
    convert_recipe_times()