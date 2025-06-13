from fastapi import APIRouter, Depends
from auth import get_user
from models import Recipe

router = APIRouter()

@router.get("/", include_in_schema=False)
async def get_testroute(user: dict = Depends(get_user)):
    return user

@router.post("/recipe", response_model=Recipe)
async def create_recipe(Recipe: Recipe):
    pass

@router.post("/product")
async def create_product(product: dict):
    pass
