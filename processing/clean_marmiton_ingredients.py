from pymongo import MongoClient
import re
import os

STOPWORDS = {
    "de", "du", "des", "d'", "la", "le", "les", "l'", "en", "avec", "et", "à", "au", "aux", "un", "une"
}

UNITES = [
    "g", "grammes", "kg", "mg", "ml", "cl", "l",
    "cuillère", "cuillères", "soupe", "café",
    "pincée", "pincées", "verre", "verres", "centimètre", "centimètres", "cm",
    "tranche", "tranches", "boîte", "boîtes", "sachet", "sachets", "pot", "pots", "filet", "filets"
]

def nettoyer_ingredient(texte):
    texte = texte.lower()
    # Supprimer les quantités et unités
    pattern_unit = r"\b\d+([.,]\d+)?\s*(" + "|".join(UNITES) + r")\b"
    texte = re.sub(pattern_unit, "", texte)
    # Supprimer les chiffres
    texte = re.sub(r"\d+([.,]\d+)?", "", texte)
    # Supprimer les caractères spéciaux
    texte = re.sub(r"[^a-zàâäéèêëïîôöùûüç\s-]", "", texte)
    # Supprimer les stopwords
    mots = texte.split()
    mots_nettoyes = [mot for mot in mots if mot not in STOPWORDS]
    # Normalisation
    mots_nettoyes = [re.sub(r"(s|x)$", "", mot) for mot in mots_nettoyes]
    return " ".join(mots_nettoyes).strip()

def extraire_ingredients_mongo():
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
    db = client["OpenFoodImpact"]
    collection = db["recipes"]
    ingredients_set = set()
    for doc in collection.find({}, {"recipeIngredient": 1}):
        if "recipeIngredient" in doc and isinstance(doc["recipeIngredient"], list):
            for item in doc["recipeIngredient"]:
                ingr = nettoyer_ingredient(item)
                if ingr:
                    ingredients_set.add(ingr)
    return sorted(ingredients_set)

if __name__ == "__main__":
    ingredients_nettoyes = extraire_ingredients_mongo()

    for ing in ingredients_nettoyes:
        print(ing)
