import pytest
import pandas as pd
from processing import utils
from processing import agribalyse_api
from processing import openfoodfacts_script

def test_normalize_name():
    """
    Teste la normalisation d'un nom (accents, espaces, etc).

    Args:
        Aucun
    Returns:
        None
    """
    assert utils.normalize_name('Éléphant!') == 'elephant'
    assert utils.normalize_name('Crème brûlée') == 'creme brulee'
    assert utils.normalize_name('  Test 123 ') == 'test 123'
    assert utils.normalize_name('') == ''
    assert utils.normalize_name(None) == ''

def test_vectorize_name_shape():
    """
    Teste la vectorisation d'un nom (dimension du vecteur).

    Args:
        Aucun
    Returns:
        None
    """
    vec = utils.vectorize_name('banane')
    assert isinstance(vec, list)
    assert len(vec) in (384, 768)  # Model size

def test_transform_agribalyse_record():
    """
    Teste la transformation d'un enregistrement Agribalyse brut.

    Args:
        Aucun
    Returns:
        None
    """
    raw = {'Code_AGB': '123', 'Nom_du_Produit_en_Français': 'Pomme'}
    clean = agribalyse_api.transform_agribalyse_record(raw)
    assert 'code_agb' in clean
    assert 'nom_produit_francais' in clean
    assert clean['code_agb'] == '123'
    assert clean['nom_produit_francais'] == 'Pomme'

def test_extract_openfoodfacts_chunks():
    """
    Teste l'extraction de chunks OpenFoodFacts (retourne un générateur de DataFrame).

    Args:
        Aucun
    Returns:
        None
    """
    gen = openfoodfacts_script.extract_openfoodfacts_chunks()
    chunk = next(gen)
    assert isinstance(chunk, pd.DataFrame)
    assert 'product_name' in chunk.columns
    assert 'code' in chunk.columns

def test_etl_openfoodfacts_runs(monkeypatch):
    """
    Teste que l'ETL OpenFoodFacts parcourt les chunks sans lever d'exception (DB mockée).

    Args:
        monkeypatch: fixture pytest pour patcher les fonctions
    Returns:
        None
    """
    # Patch extract to yield one small chunk, patch load to just count
    df = pd.DataFrame({"code": ["1"], "product_name": ["Test"], "countries_tags": ["en:france"]})
    monkeypatch.setattr(openfoodfacts_script, "extract_openfoodfacts_chunks", lambda: iter([df]))
    monkeypatch.setattr(openfoodfacts_script, "load_openfoodfacts_chunk_to_db", lambda chunk: None)
    openfoodfacts_script.pipeline_openfoodfacts()  # Should not raise

def test_handle_error_logs_and_raises():
    """
    Teste que handle_error log et lève bien une exception.

    Args:
        Aucun
    Returns:
        None
    """
    import logging
    with pytest.raises(Exception):
        utils.handle_error(Exception('test'), context='unit')

def test_extract_agribalyse_data(monkeypatch):
    """
    Teste l'extraction Agribalyse (mock API).

    Args:
        monkeypatch: fixture pytest pour patcher requests.get
    Returns:
        None
    """
    monkeypatch.setattr('requests.get', lambda url: type('resp', (), { 'raise_for_status': lambda self: None, 'json': lambda self: {"results": [{"Code_AGB": "1", "Nom_du_Produit_en_Français": "Test"}], "next": None } })())
    data = agribalyse_api.extract_agribalyse_data()
    assert isinstance(data, list)
    assert data and 'Code_AGB' in data[0]

def test_load_agribalyse_data_to_db_handles_none(monkeypatch):
    """
    Teste que la fonction lève une exception si la connexion est None.

    Args:
        monkeypatch: fixture pytest pour patcher get_db_connection
    Returns:
        None
    """
    monkeypatch.setattr("processing.agribalyse_api.get_db_connection", lambda: None)
    with pytest.raises(Exception, match="Database connection is None"):
        agribalyse_api.load_agribalyse_data_to_db([{"code_agb": "1", "nom_produit_francais": "Test"}])

def test_etl_agribalyse_runs(monkeypatch):
    """
    Teste que l'ETL Agribalyse fonctionne avec des mocks.

    Args:
        monkeypatch: fixture pytest pour patcher les fonctions
    Returns:
        None
    """
    monkeypatch.setattr(agribalyse_api, "extract_agribalyse_data", lambda: [{"code_agb": "1", "nom_produit_francais": "Test"}])
    monkeypatch.setattr(agribalyse_api, "load_agribalyse_data_to_db", lambda data: None)
    agribalyse_api.pipeline_agribalyse()  # Should not raise

def test_scrape_greenpeace_calendar(monkeypatch):
    """
    Teste le scraping du calendrier Greenpeace (mock HTML).

    Args:
        monkeypatch: fixture pytest pour patcher requests.get
    Returns:
        None
    """
    html = '''<div class="month"><a name="janvier"></a><ul class="list-legumes"><li>Carotte</li></ul></div>'''
    monkeypatch.setattr('requests.get', lambda url: type('resp', (), { 'raise_for_status': lambda self: None, 'content': html.encode('utf-8') })())
    from processing import scraping_greenpeace
    cal = scraping_greenpeace.scrape_greenpeace_calendar()
    assert isinstance(cal, dict)
    assert 'janvier' in cal and 'Carotte' in cal['janvier']

def test_insert_season_data_to_db_handles_none(monkeypatch):
    """
    Teste que l'insertion Greenpeace gère une connexion None sans erreur.

    Args:
        monkeypatch: fixture pytest pour patcher get_db_connection
    Returns:
        None
    """
    from processing import scraping_greenpeace
    monkeypatch.setattr(scraping_greenpeace, "get_db_connection", lambda: None)
    scraping_greenpeace.insert_season_data_to_db({'janvier': ['Carotte']})  # Should not raise

def test_scrapes_recipe_list(monkeypatch):
    """
    Teste le scraping de la liste de recettes Marmiton (mock HTML).

    Args:
        monkeypatch: fixture pytest pour patcher requests.get
    Returns:
        None
    """
    html = '''<div class="type-Recipe"><div class="mrtn-card__title">Tarte</div><a href="/recette/1"></a></div>'''
    monkeypatch.setattr('requests.get', lambda url, timeout=10: type('resp', (), { 'raise_for_status': lambda self: None, 'content': html.encode('utf-8') })())
    from processing import scraping_marmiton
    recipes = scraping_marmiton.scrapes_recipe_list()
    assert isinstance(recipes, list)
    assert recipes and recipes[0]['title'] == 'Tarte'

def test_extract_schemaorg_recipe(monkeypatch):
    """
    Teste l'extraction schema.org d'une recette Marmiton (mock HTML/JSON).

    Args:
        monkeypatch: fixture pytest pour patcher requests.get
    Returns:
        None
    """
    html = '<script type="application/ld+json">{"@type": "Recipe", "name": "Tarte"}</script>'
    monkeypatch.setattr('requests.get', lambda url, timeout=10: type('resp', (), { 'raise_for_status': lambda self: None, 'content': html.encode('utf-8') })())
    from processing import scraping_marmiton
    data = scraping_marmiton.extract_schemaorg_recipe('dummy')
    assert isinstance(data, dict)
    assert data['name'] == 'Tarte'

def test_insert_recipes_handles_empty(monkeypatch):
    """
    Teste que l'insertion MongoDB gère une liste vide sans erreur.

    Args:
        monkeypatch: fixture pytest pour patcher MongoClient
    Returns:
        None
    """
    from processing import scraping_marmiton
    class DummyCollection:
        def insert_many(self, recipes, ordered):
            return None
    class DummyDB:
        def __getitem__(self, name):
            return DummyCollection()
    class DummyClient:
        def __getitem__(self, name):
            return DummyDB()
    monkeypatch.setattr('pymongo.MongoClient', lambda *a, **kw: DummyClient())
    scraping_marmiton.insert_recipes([])  # Should not raise

def test_extract_all_recipes(monkeypatch):
    """
    Teste l'extraction complète Marmiton (mocks).

    Args:
        monkeypatch: fixture pytest pour patcher les fonctions
    Returns:
        None
    """
    from processing import scraping_marmiton
    monkeypatch.setattr(scraping_marmiton, "scrapes_recipe_list", lambda: [{"title": "Tarte", "link": "dummy"}])
    monkeypatch.setattr(scraping_marmiton, "extract_schemaorg_recipe", lambda url: {"@type": "Recipe", "name": "Tarte"})
    monkeypatch.setattr(scraping_marmiton, "remove_objectid", lambda data: data)
    monkeypatch.setattr(scraping_marmiton, "insert_recipes", lambda recipes: None)
    recipes = scraping_marmiton.extract_all_recipes()
    assert isinstance(recipes, list)
    assert recipes and recipes[0].get("title") == "Tarte"
