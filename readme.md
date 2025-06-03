# DataFoodImpact: Une base de données pour une alimentation responsable

Ce projet vise à créer une base de données centralisant les informations nutritionnelles, environnementales et saisonnières des aliments, ainsi que des recettes.  L'objectif est d'aider les utilisateurs à faire des choix alimentaires plus responsables et personnalisés.

## Objectifs

* Collecter et agréger des données nutritionnelles, environnementales et saisonnières.
* Créer une base de données relationnelle et NoSQL.
* Développer une API REST pour accéder aux données.
* Fournir des outils pour calculer l'empreinte carbone des repas et proposer des recettes personnalisées.

## Structure du projet

```txt
Readme/
├── data/ # fichies de données temporaires
├── db/ # scripts de création de la base de données
├── docs/ # documentation du projet
├── notebooks/ # notebooks de tests divers
├── processing/ # scripts d'extraction et préparation des données avant insertion en base de données
├── tests/ # tests unitaires et tests d'intégration de l'api
├── api/ 
├── requirements.txt
└── README.md
```

## Démarrage rapide

### Prerequisites

### Prérequis

* Python 3.x
* `pip`
* Un environnement virtuel (recommandé)
* [Docker](https://www.docker.com/) et [Docker Compose](https://docs.docker.com/compose/) pour la gestion des bases de données (PostgreSQL/pgvector et MongoDB)

### Installation

1. Cloner le répertoire
```bash
git clone https://github.com/remijul/2025_SpamClassifier.git
cd 2025_SpamClassifier
```

2. Créer et activer l'environnement virtuel
```bash
python -m venv venv

venv\Scripts\activate
```

3. Installer les dépendances
```bash
pip install -r requirements.txt
```

4. Lancer les bases de données avec Docker Compose
```bash
docker compose up -d
```
Cela démarre les services PostgreSQL/pgvector et MongoDB avec persistance des données dans les dossiers `pgvector_data/` et `mongodb_data/` du projet (ces dossiers sont à ignorer dans git).

### Workflow du projet

1. **Collecte des données:**  Extraction de données depuis Open Food Facts, Agribalyse (API), Greenpeace, Marmiton (scraping), et fichiers locaux.
2. **Nettoyage et préparation des données:**  Homogénéisation des formats, gestion des données manquantes, suppression des données corrompues.
3. **Création de la base de données:**  Conception et implémentation d'une base de données relationnelle (PostgreSQL ou MySQL) et d'une base NoSQL (MongoDB).
4. **Développement de l'API:**  Création d'une API REST en FastAPI avec une documentation OpenAPI.

## Améliorations possibles

* Intégration de modèles d'IA pour des recommandations plus personnalisées.
* Amélioration de l'interface utilisateur pour une meilleure expérience.
* Extension des sources de données.

## Licence

Ce projet est sous licence MIT - voir le fichier LICENSE pour plus de détails.

## Support

Pour toute question ou problème:
1. Vérifiez la section de dépannage
2. Ouvrez un problème dans le référentiel

```SQL
-- Table centrale pour la vectorisation des noms
CREATE TABLE product_vector (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    name_vector VECTOR(384), -- ou VECTOR(768) selon le modèle utilisé
    source VARCHAR(32) NOT NULL, -- 'greenpeace', 'agribalyse', 'openfoodfacts'
    source_id TEXT, -- identifiant unique dans la source d'origine
    extra JSONB -- métadonnées additionnelles (optionnel)
);

CREATE TABLE agribalyse (
    id SERIAL PRIMARY KEY,
    product_vector_id INTEGER REFERENCES product_vector(id),
    code_agb TEXT,
    code_ciqual TEXT,
    groupe_aliment TEXT,
    sous_groupe_aliment TEXT,
    lci_name TEXT,
    score_unique_ef FLOAT,
    changement_climatique FLOAT,
    -- ... autres colonnes d'impact environnemental ...
    data JSONB -- pour stocker les autres champs bruts si besoin
);

CREATE TABLE openfoodfacts (
    id SERIAL PRIMARY KEY,
    product_vector_id INTEGER REFERENCES product_vector(id),
    code TEXT,
    product_name TEXT,
    brands TEXT,
    categories TEXT,
    nutriscore_score FLOAT,
    nutriscore_grade TEXT,
    nova_group INTEGER,
    -- ... autres colonnes nutritionnelles ...
    data JSONB -- pour stocker les autres champs bruts si besoin
);

CREATE TABLE greenpeace_season (
    id SERIAL PRIMARY KEY,
    product_vector_id INTEGER REFERENCES product_vector(id),
    month VARCHAR(16),
    is_seasonal BOOLEAN
);
```