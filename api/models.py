from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class Recipe(BaseModel):
    id: Optional[str] = Field(default=None, description="MongoDB ObjectId as string")
    title: str
    link: Optional[str] = None
    recipeCategory: Optional[str] = None
    image: Optional[List[str]] = None
    datePublished: Optional[str] = None
    prepTime: Optional[str] = None
    cookTime: Optional[str] = None
    totalTime: Optional[str] = None
    recipeYield: Optional[str] = None
    recipeIngredient: Optional[List[str]]
    recipeInstructions: Optional[List[str]]
    author: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[str] = None
    recipeCuisine: Optional[str] = None
    RelatedLink: Optional[List[str]] = []
    normalized_ingredients: Optional[List[str]] = None