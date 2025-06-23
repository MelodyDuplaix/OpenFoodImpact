# DataFoodImpact : Base de données et API pour une alimentation responsable

Ce projet vise à centraliser et croiser des informations nutritionnelles, environnementales, saisonnières et culinaires, afin d’accompagner les utilisateurs vers des choix alimentaires plus responsables, personnalisés et informés.

## Objectifs

- Collecter et agréger des données issues de sources variées (Open Food Facts, Agribalyse, Greenpeace, Marmiton…)
- Nettoyer, normaliser et relier ces données pour constituer un jeu de données unique et interrogeable
- Stocker les données structurées dans PostgreSQL (avec pgvector et pg_trgm) et les recettes dans MongoDB (structure flexible)
- Développer une API REST sécurisée (FastAPI) pour exposer les données et fonctionnalités (recherche, analyse de similarité, recommandations…)
- Automatiser l’ensemble du pipeline (extraction, traitement, insertion, déploiement)

## Démarrage rapide

### Prérequis

* Python 3.10
* `pip`
* Un environnement virtuel (recommandé)
* [Docker](https://www.docker.com/) et [Docker Compose](https://docs.docker.com/compose/) pour la gestion des bases de données (PostgreSQL/pgvector et MongoDB)

### Installation

1. Cloner le répertoire
```bash
git clone https://github.com/MelodyDuplaix/projet_certif_OpenFoodImpact.git
cd projet_certif_OpenFoodImpact
```
2. Créer et activer l'environnement virtuel
```bash
python -m venv venv
venv\Scripts\activate # source venv/bin/activate sous Linux
```
3. Installer les dépendances
```bash
pip install -r requirements.txt
```
4. Lancer les bases de données
```bash
docker compose up -d
```
Cela démarre les services PostgreSQL/pgvector et MongoDB avec persistance des données dans les dossiers `pgvector_data/` et `mongodb_data/` du projet (ces dossiers sont à ignorer dans git).

5. Intilialiser la base de données et l'extraction des données
```bash
python processing/main_pipeline.py
```

6. Lancer l'API
```bash
python api/main.py
```

### Sauvegarde (backup) des bases de données
- Un script `processing/backup_databases.py` permet de sauvegarder à la demande l’intégralité des bases PostgreSQL et MongoDB (dumps SQL et BSON dans `data/backups/`).
- Ce script utilise `pg_dump` et `mongodump` (à installer sur la machine ou dans le conteneur) et permet de restaurer ou migrer facilement les données.
- Lancer simplement :
```bash
python processing/backup_databases.py
```

## Fonctionnalités principales

- **Extraction multi-source** : API, fichiers, web scraping (scripts dédiés)
- **Nettoyage & agrégation** : homogénéisation, gestion des valeurs manquantes, vectorisation (Hugging Face), matching fuzzy (pg_trgm)
- **Stockage** :
  - PostgreSQL + pgvector : produits, ingrédients, liens de similarité, données environnementales/nutritionnelles
  - MongoDB : recettes Marmiton (structure flexible)
- **API REST** :
  - Recherche de produits, ingrédients, recettes (filtres avancés)
  - Analyse de similarité (vectorielle et textuelle)
  - Authentification JWT (routes sécurisées pour ajout/modification)
  - Documentation interactive Swagger/OpenAPI
- **Automatisation** : pipeline complet orchestré par scripts Python et Docker Compose

## Création d'un utilisateur admin

Pour créer un compte administrateur dans la base de données, exécutez la commande suivante :

```bash
python api/db.py
```

## Structure du projet

```txt
projet_certif_cooking/
├── api/                # API FastAPI (routes, modèles, services, auth)
├── processing/         # Scripts d’extraction, nettoyage, agrégation, vectorisation
├── data/               # Données brutes et intermédiaires
├── db/                 # Fichiers et scripts liés aux bases de données
├── docs/               # Documentation technique et rapports
├── notebooks/          # Analyses et tests exploratoires
├── tests/              # Tests unitaires et d’intégration
├── requirements.txt    # Dépendances Python
├── docker-compose.yml  # Orchestration des bases PostgreSQL/pgvector et MongoDB
└── readme.md
```

## Automatisation, gestion des données & sécurité

- Pipeline automatisé : extraction multi-source, nettoyage, homogénéisation, vectorisation (Hugging Face), insertion en base
- Sécurité :
  - Authentification bases via Docker Compose
  - API sécurisée JWT (routes publiques/privées)
  - ORM (SQLAlchemy, pymongo) pour requêtes sûres
- Sélection/filtrage :
  - OpenFoodFacts : filtrage France, gestion valeurs manquantes, analyse nullité (voir `processing/null_percent_report.py`)
  - Agribalyse/Greenpeace : mapping, typage, normalisation(pg_trgm), similarité vectorielle (pgvector)
- Jointures & optimisations :
  - Insertion par lots, index, gestion des conflits, analyse nullité
  - Pré-calcul des liens d’équivalence entre sources (voir `processing/ingredient_similarity.py`)

## Agrégation : dépendances, nettoyage, homogénéisation

- Dépendances : sentence-transformers (vectorisation), pgvector, pg_trgm (similarité)
- Nettoyage : uniformisation des champs, gestion des valeurs manquantes, normalisation des noms
- Homogénéisation : mapping des colonnes, typage, suppression des doublons
- Vectorisation : embeddings via Hugging Face pour les noms d’ingrédients/produits
- Matching : fuzzy matching (pg_trgm) + similarité vectorielle (pgvector)
- Scripts clés : `processing/ingredient_similarity.py`, `processing/clean_marmiton_ingredients.py`, `processing/clean_recipes_times.py`

## Création de la base : dépendances, commandes, conformité RGPD

- Dépendances : psycopg2, pymongo, SQLAlchemy, extensions PostgreSQL (pgvector, pg_trgm)
- Commandes :
  - Création/initialisation : `python processing/main_pipeline.py`
  - Sauvegarde/restauration : `python processing/backup_databases.py`
  - Création admin : `python api/db.py`
- Conformité RGPD :
  - Données publiques uniquement, stockage local, anonymisation
  - Scripts de backup/restauration pour portabilité et sécurité

## API : endpoints, authentification, logique, dépendances

- Endpoints principaux :
  - Recherche produits, ingrédients, recettes (filtres avancés)
  - Analyse de similarité (vectorielle et textuelle)
  - Recommandations
  - Ajout/modification (routes sécurisées)
- Authentification : JWT (création, login, accès routes privées)
- Sécurité : hashing mots de passe (bcrypt, passlib), validation stricte des entrées
- Documentation interactive : Swagger/OpenAPI (auto-générée)
- Dépendances : FastAPI, SQLAlchemy, passlib, bcrypt, pydantic
- Logique :
  - Utilisation ORM pour requêtes sécurisées
  - Nettoyage et homogénéisation des formats en entrée/sortie
  - Enchaînement logique : extraction → nettoyage → vectorisation → insertion → API

## Principales dépendances

- FastAPI, SQLAlchemy, psycopg2, pymongo, sentence-transformers, scikit-learn, passlib, bcrypt, requests, beautifulsoup4, matplotlib

## Commandes utiles

- Lancer pipeline : `python processing/main_pipeline.py`
- Lancer API : `python api/main.py`
- Sauvegarde : `python processing/backup_databases.py`
- Créer admin : `python api/db.py`

Pour plus de détails, voir les scripts et la documentation technique dans `docs/`.
