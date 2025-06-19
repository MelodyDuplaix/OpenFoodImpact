from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class HowToStep(BaseModel):
    type_: str = Field(default="HowToStep", alias="@type")
    text: str

class AggregateRating(BaseModel):
    type_: str = Field(default="AggregateRating", alias="@type")
    reviewCount: Optional[int]
    ratingValue: Optional[float]
    worstRating: Optional[int]
    bestRating: Optional[int]

class VideoObject(BaseModel):
    type_: str = Field(default="VideoObject", alias="@type")
    name: Optional[str]
    description: Optional[str]
    thumbnailUrl: Optional[List[str]]
    contentUrl: Optional[str]
    embedUrl: Optional[str]
    uploadDate: Optional[str]

class ParsedIngredientDetail(BaseModel):
    raw_text: str
    quantity_str: Optional[str]
    unit_str: Optional[str]
    parsed_name: Optional[str]
    quantity_grams: Optional[float]
    normalized_name_for_matching: Optional[str]

class RecipeCreate(BaseModel):
    title: str
    link: Optional[str]
    category: Optional[str]
    context: Optional[str] = Field(default="http://schema.org", alias="@context")
    type_: Optional[str] = Field(default="Recipe", alias="@type")
    name: str
    recipeCategory: Optional[str]
    image: Optional[List[str]]
    datePublished: Optional[str]
    prepTime: Optional[int]
    cookTime: Optional[int]
    totalTime: Optional[int]
    recipeYield: Optional[str]
    recipeIngredient: List[str]
    recipeInstructions: List[HowToStep]
    author: Optional[str]
    description: Optional[str]
    keywords: Optional[str]
    recipeCuisine: Optional[str]
    aggregateRating: Optional[AggregateRating]
    video: Optional[VideoObject]
    normalized_ingredients: Optional[List[str]] = None
    parsed_ingredients_details: Optional[List[ParsedIngredientDetail]] = None
