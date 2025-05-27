import pandas as pd
from bs4 import BeautifulSoup
import requests
import json

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

if __name__ == "__main__":
    print("Scraping Greenpeace calendar...")
    calendar_data = scrape_greenpeace_calendar()
    print("saving data to CSV...")
    with open("data/greenpeace_seasons.json", mode="w", encoding="utf-8") as f:
        json.dump(calendar_data, f, ensure_ascii=False, indent=4)
    print("Scraping completed.")
    print(calendar_data)