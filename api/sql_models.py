from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint, Text
from sqlalchemy.orm import relationship, declarative_base
from pgvector.sqlalchemy import Vector

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    password = Column(String, nullable=False) # Hashed password
    user_level = Column(String, nullable=False, default="user")

class ProductVector(Base):
    __tablename__ = "product_vector"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=False)
    name_vector = Column(Vector(384))
    source = Column(String(32), nullable=False)
    code_source = Column(Text)

    __table_args__ = (UniqueConstraint('name', 'source', name='uq_product_vector_name_source'),)

    agribalyse_entries = relationship("Agribalyse", back_populates="product_vector_item", cascade="all, delete-orphan")
    openfoodfacts_entries = relationship("OpenFoodFacts", back_populates="product_vector_item", cascade="all, delete-orphan")
    greenpeace_season_entries = relationship("GreenpeaceSeason", back_populates="product_vector_item", cascade="all, delete-orphan")
    
    source_links = relationship(
        "IngredientLink",
        foreign_keys="[IngredientLink.id_source]",
        back_populates="source_product",
        cascade="all, delete-orphan"
    )
    linked_links = relationship(
        "IngredientLink",
        foreign_keys="[IngredientLink.id_linked]",
        back_populates="linked_product",
        cascade="all, delete-orphan"
    )

class Agribalyse(Base):
    __tablename__ = "agribalyse"
    id = Column(Integer, primary_key=True, index=True)
    product_vector_id = Column(Integer, ForeignKey("product_vector.id", ondelete="CASCADE"), index=True)
    code_agb = Column(Text, index=True)
    code_ciqual = Column(Text, index=True)
    lci_name = Column(Text)
    nom_produit_francais = Column(Text)
    changement_climatique = Column(Float)
    score_unique_ef = Column(Float)
    ecotoxicite_eau_douce = Column(Float)
    epuisement_ressources_energetiques = Column(Float)
    eutrophisation_marine = Column(Float)
    sous_groupe_aliment = Column(Text)
    effets_tox_cancerogenes = Column(Float)
    approche_emballage = Column(Text)
    epuisement_ressources_eau = Column(Float)
    eutrophisation_terrestre = Column(Float)
    utilisation_sol = Column(Float)
    code_avion = Column(Text)
    effets_tox_non_cancerogenes = Column(Float)
    epuisement_ressources_mineraux = Column(Float)
    particules_fines = Column(Float)
    formation_photochimique_ozone = Column(Float)
    livraison = Column(Text)
    preparation = Column(Text)
    changement_climatique_biogenique = Column(Float)
    acidification_terrestre_eaux_douces = Column(Float)
    groupe_aliment = Column(Text)
    changement_climatique_cas = Column(Float)
    appauvrissement_couche_ozone = Column(Float)
    rayonnements_ionisants = Column(Float)
    eutrophisation_eaux_douces = Column(Float)
    changement_climatique_fossile = Column(Float)

    product_vector_item = relationship("ProductVector", back_populates="agribalyse_entries")

class OpenFoodFacts(Base):
    __tablename__ = "openfoodfacts"
    id = Column(Integer, primary_key=True, index=True)
    product_vector_id = Column(Integer, ForeignKey("product_vector.id", ondelete="CASCADE"), index=True)
    code = Column(Text, index=True)
    product_name = Column(Text, index=True)
    brands = Column(Text)
    categories = Column(Text)
    labels_tags = Column(Text)
    packaging_tags = Column(Text)
    image_url = Column(Text)
    energy_kcal_100g = Column(Float)
    fat_100g = Column(Float)
    saturated_fat_100g = Column(Float)
    carbohydrates_100g = Column(Float)
    sugars_100g = Column(Float)
    fiber_100g = Column(Float)
    proteins_100g = Column(Float)
    salt_100g = Column(Float)
    nutriscore_score = Column(Float)
    nutriscore_grade = Column(Text)
    nova_group = Column(Integer)
    environmental_score_score = Column(Float)
    environmental_score_grade = Column(Text)
    ingredients_text = Column(Text)
    ingredients_analysis_tags = Column(Text)
    additives_tags = Column(Text)
    sodium_100g = Column(Float)

    product_vector_item = relationship("ProductVector", back_populates="openfoodfacts_entries")

class GreenpeaceSeason(Base):
    __tablename__ = "greenpeace_season"
    id = Column(Integer, primary_key=True, index=True)
    product_vector_id = Column(Integer, ForeignKey("product_vector.id", ondelete="CASCADE"), index=True)
    month = Column(Text)

    product_vector_item = relationship("ProductVector", back_populates="greenpeace_season_entries")

class IngredientLink(Base):
    __tablename__ = "ingredient_link"
    id_source = Column(Integer, ForeignKey("product_vector.id", ondelete="CASCADE"), primary_key=True, index=True)
    source = Column(Text, primary_key=True, index=True)
    id_linked = Column(Integer, ForeignKey("product_vector.id", ondelete="CASCADE"), primary_key=True, index=True)
    linked_source = Column(Text, primary_key=True, index=True)
    score = Column(Float, index=True)

    source_product = relationship("ProductVector", foreign_keys=[id_source], back_populates="source_links")
    linked_product = relationship("ProductVector", foreign_keys=[id_linked], back_populates="linked_links")
