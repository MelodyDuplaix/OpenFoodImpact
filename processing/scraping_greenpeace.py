import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import os
import psycopg2
from sentence_transformers import SentenceTransformer
import numpy as np
import unicodedata
import re
from .utils import get_db_connection, normalize_name, vectorize_name, safe_execute

greenpeace_url = "https://www.greenpeace.fr/guetteur/calendrier/"

def scrape_greenpeace_calendar():
    """
    Scrapes the Greenpeace calendar of fruits and vegetables seasons.

    Returns:
        dict: A dictionary where keys are months and values are lists of seasonal fruits and vegetables.
    """
    response = requests.get(greenpeace_url)
    soup = BeautifulSoup(response.content, "html.parser")
    calendrier = {}
    for mois_section in soup.select(".month"):
        month = mois_section.select_one("a")
        if not month:
            continue
        month = month["name"]
        legumes = mois_section.select(".list-legumes")
        legumes_list = [legume.get_text(strip=True) for legume in legumes[0].find_all("li")] if legumes else []
        calendrier[month] = legumes_list
    
    return calendrier

def vectorize_product_name(name):
    """
    Vectorize the name of a product using sentence-transformers.

    Args:
        name (string): The name of the product to vectorize.
        
    Returns:
        list: A list of floats representing the vectorized name.
    """
    # Load model only once (cache as attribute)
    if not hasattr(vectorize_product_name, "model"):
        vectorize_product_name.model = SentenceTransformer('all-MiniLM-L6-v2')
    embedding = vectorize_product_name.model.encode([name])[0]
    return embedding.tolist()

def insert_season_data_to_db(season_data):
    conn = get_db_connection()
    cur = conn.cursor()
    for month, items in season_data.items():
        for name in items:
            name_normalized = normalize_name(name)
            name_vector = vectorize_name(name_normalized)
            # Insert into product_vector
            safe_execute(cur, """
                INSERT INTO product_vector (name, name_vector, source)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id;
            """, (name_normalized, name_vector, 'greenpeace'))
            result = cur.fetchone()
            if result:
                product_vector_id = result[0]
            else:
                safe_execute(cur, "SELECT id FROM product_vector WHERE name = %s AND source = %s;", (name_normalized, 'greenpeace'))
                fetch = cur.fetchone()
                if not fetch:
                    continue
                product_vector_id = fetch[0]
            # Insert into greenpeace_season
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