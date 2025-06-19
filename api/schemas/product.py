from pydantic import BaseModel, Field, validator
from typing import List, Optional, Literal

class AgribalyseProductData(BaseModel):
    """
    Payload for Agribalyse-specific product data.
    'nom_produit_francais' comes from the top-level 'name'.
    'code_agb' comes from the top-level 'code_source'.
    """
    nom_produit_francais_agb: Optional[str] = Field(None, description="Specific French name for Agribalyse. If None, uses the main product name.")
    code_agb: Optional[str] = Field(None, description="Specific Agribalyse code (Code AGB). If None, uses the main product code_source if source is 'agribalyse'.")
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
    Payload for OpenFoodFacts-specific product data.
    'product_name' comes from the top-level 'name'.
    'code' (barcode) comes from the top-level 'code_source'.
    """
    product_name_off: Optional[str] = Field(None, description="Specific product name for OpenFoodFacts. If None, uses the main product name.")
    code_off: Optional[str] = Field(None, description="Specific OpenFoodFacts code (barcode). If None, uses the main product code_source if source is 'openfoodfacts'.")
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
    Payload for Greenpeace-specific product data.
    'name' comes from the top-level 'name'.
    """
    months: List[str] = Field(..., example=["janvier", "février"], description="List of months the product is in season") # type: ignore

class ProductCreate(BaseModel):
    name: str = Field(..., example="Pomme de terre", description="The common name of the product.") # type: ignore
    # source and code_source will be determined by the endpoint based on existing data or provided payloads.

    agribalyse_payload: Optional[AgribalyseProductData] = None
    openfoodfacts_payload: Optional[OpenFoodFactsProductData] = None
    greenpeace_payload: Optional[GreenpeaceProductData] = None


class ProductCreationResponse(BaseModel):
    product_vector_id: int
    name: str
    normalized_name: str
    source: str
    code_source: Optional[str]
    message: str
