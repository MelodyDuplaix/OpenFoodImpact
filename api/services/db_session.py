import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession # Renamed to avoid conflict
from typing import Generator
from api.sql_models import Base # Import Base from your models file

POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')

SQLALCHEMY_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """
    Initialise la base de données en créant toutes les tables définies dans les modèles SQLAlchemy.
    Cette fonction doit être appelée une fois au démarrage de l'application si les tables n'existent pas.
    """
    Base.metadata.create_all(bind=engine)

def get_db() -> Generator[SQLAlchemySession, None, None]:
    """
    Dépendance FastAPI pour obtenir une session de base de données.
    Gère l'ouverture et la fermeture de la session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
