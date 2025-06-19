# DataFoodImpact: Une base de données pour une alimentation responsable

Ce projet vise à centraliser et croiser des informations nutritionnelles, environnementales, saisonnières et culinaires, afin d’accompagner les utilisateurs vers des choix alimentaires plus responsables, personnalisés et informés.

## Objectifs

- Collecter et agréger des données issues de sources variées (Open Food Facts, Agribalyse, Greenpeace, Marmiton…)
- Nettoyer, normaliser et relier ces données pour constituer un jeu de données unique et interrogeable
- Stocker les données structurées dans PostgreSQL (avec pgvector et pg_trgm) et les recettes dans MongoDB (structure flexible)
- Développer une API REST sécurisée (FastAPI) pour exposer les données et fonctionnalités (recherche, analyse de similarité, recommandations…)
- Automatiser l’ensemble du pipeline (extraction, traitement, insertion, déploiement)

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

## Améliorations possibles

- Recommandations personnalisées (modèles IA)
- Extension à d’autres sources ou fonctionnalités (menus hebdomadaires, scoring environnemental…)
- Optimisation des performances (indexation, cache, requêtes asynchrones)
- Interface utilisateur dédiée
