from pydantic import BaseModel, Field, validator
from typing import List, Optional, Literal

class AgribalyseProductData(BaseModel):
    """
    Données spécifiques Agribalyse pour un produit.
    'nom_produit_francais' provient du champ principal 'name'.
    """
    nom_produit_francais_agb: Optional[str] = Field(None, description="Nom français spécifique pour Agribalyse. Si None, utilise le nom principal du produit.")
    code_agb: Optional[str] = Field(None, description="Code Agribalyse spécifique (Code AGB). Si None, utilise le code principal si la source est 'agribalyse'.")
    code_ciqual: Optional[str] = None
    lci_name: Optional[str] = None
    changement_climatique: Optional[float] = None
    score_unique_ef: Optional[float] = None
    ecotoxicite_eau_douce: Optional[float] = None
    epuisement_ressources_energetiques: Optional[float] = None
    eutrophisation_marine: Optional[float] = None
    sous_groupe_aliment: Optional[str] = None
    effets_tox_cancerogenes: Optional[float] = None
    approche_emballage: Optional[str] = None
    epuisement_ressources_eau: Optional[float] = None
    eutrophisation_terrestre: Optional[float] = None
    utilisation_sol: Optional[float] = None
    code_avion: Optional[str] = None
    effets_tox_non_cancerogenes: Optional[float] = None
    epuisement_ressources_mineraux: Optional[float] = None
    particules_fines: Optional[float] = None
    formation_photochimique_ozone: Optional[float] = None
    livraison: Optional[str] = None
    preparation: Optional[str] = None
    changement_climatique_biogenique: Optional[float] = None
    acidification_terrestre_eaux_douces: Optional[float] = None
    groupe_aliment: Optional[str] = None
    changement_climatique_cas: Optional[float] = None
    appauvrissement_couche_ozone: Optional[float] = None
    rayonnements_ionisants: Optional[float] = None
    eutrophisation_eaux_douces: Optional[float] = None
    changement_climatique_fossile: Optional[float] = None

class OpenFoodFactsProductData(BaseModel):
    """
    Données spécifiques OpenFoodFacts pour un produit.
    'product_name' provient du champ principal 'name'.
    """
    product_name_off: Optional[str] = Field(None, description="Nom spécifique pour OpenFoodFacts. Si None, utilise le nom principal du produit.")
    code_off: Optional[str] = Field(None, description="Code OpenFoodFacts spécifique (code-barres). Si None, utilise le code principal si la source est 'openfoodfacts'.")
    brands: Optional[str] = None
    categories: Optional[str] = None
    labels_tags: Optional[str] = None
    packaging_tags: Optional[str] = None
    image_url: Optional[str] = None
    energy_kcal_100g: Optional[float] = None
    fat_100g: Optional[float] = None
    saturated_fat_100g: Optional[float] = None
    carbohydrates_100g: Optional[float] = None
    sugars_100g: Optional[float] = None
    fiber_100g: Optional[float] = None
    proteins_100g: Optional[float] = None
    salt_100g: Optional[float] = None
    nutriscore_score: Optional[float] = None
    nutriscore_grade: Optional[str] = None
    nova_group: Optional[int] = None
    environmental_score_score: Optional[float] = None
    environmental_score_grade: Optional[str] = None
    ingredients_text: Optional[str] = None
    ingredients_analysis_tags: Optional[str] = None
    additives_tags: Optional[str] = None
    sodium_100g: Optional[float] = None

class GreenpeaceProductData(BaseModel):
    """
    Données spécifiques Greenpeace pour un produit.
    'name' provient du champ principal 'name'.
    """
    months: List[str] = Field(..., example=["janvier", "février"], description="Liste des mois où le produit est de saison") # type: ignore

class ProductCreate(BaseModel):
    name: str = Field(..., example="Pomme de terre", description="Nom commun du produit.") # type: ignore
    agribalyse_payload: Optional[AgribalyseProductData] = None
    openfoodfacts_payload: Optional[OpenFoodFactsProductData] = None
    greenpeace_payload: Optional[GreenpeaceProductData] = None

class ProductCreationResponse(BaseModel):
    product_vector_id: int
    name: str
    normalized_name: str
    source: str
    message: str
