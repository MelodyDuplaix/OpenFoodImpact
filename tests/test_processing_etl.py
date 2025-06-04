"""Unit tests for ETL processing scripts (agribalyse, openfoodfacts, utils).

Run with: pytest tests/test_processing_etl.py
"""
import pytest
import pandas as pd
from processing import utils
from processing import agribalyse_api
from processing import openfoodfacts_script

def test_normalize_name():
    assert utils.normalize_name('Éléphant!') == 'elephant'
    assert utils.normalize_name('Crème brûlée') == 'creme brulee'
    assert utils.normalize_name('  Test 123 ') == 'test 123'
    assert utils.normalize_name('') == ''
    assert utils.normalize_name(None) == ''

def test_vectorize_name_shape():
    vec = utils.vectorize_name('banane')
    assert isinstance(vec, list)
    assert len(vec) in (384, 768)  # Model size

def test_transform_agribalyse_record():
    raw = {'Code_AGB': '123', 'Nom_du_Produit_en_Français': 'Pomme'}
    clean = agribalyse_api.transform_agribalyse_record(raw)
    assert 'code_agb' in clean
    assert 'nom_produit_francais' in clean
    assert clean['code_agb'] == '123'
    assert clean['nom_produit_francais'] == 'Pomme'

def test_transform_openfoodfacts_row():
    row = pd.Series({'product_name': 'Banane', 'code': 'X1'})
    result = openfoodfacts_script.transform_openfoodfacts_row(row)
    assert result['name_normalized'] == 'banane'
    assert isinstance(result['name_vector'], list)
    assert result['code'] == 'X1'
    assert result['row']['product_name'] == 'Banane'

def test_handle_error_logs_and_raises():
    import logging
    with pytest.raises(Exception):
        utils.handle_error(Exception('test'), context='unit')
