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

5. Intilialiser la base de données et l'extraction des données
```bash
python processing/main_pipeline.py
```

6. Lancer l'API
```bash
python api/main.py
```

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
