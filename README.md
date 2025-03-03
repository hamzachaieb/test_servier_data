# SERVIER : TESTS PYTHON & SQL

Ce projet fournit une solution pour le test technique de Servier. L'objectif principal est d'analyser les données des journaux pour identifier les mentions de médicaments dans les articles de PubMed et les études cliniques. Le projet est structuré avec plusieurs modules Python, des fichiers de configuration et des scripts de test.

# Table des matières :

- [Partie 1 - Pipeline de données](#Partie-1---pipeline-de-données)
  - [Structure du projet](#structure-du-projet)
  - [Installation](#installation)
  - [Commandes](#commandes)
  - [Production](#production)
    - [A. Déploiement](#a-déploiement)
    - [B. Orchestration](#b-orchestration)
  - [Comment adapter le code pour le Big Data](#comment-adapter-le-code-pour-le-big-data)
- [Partie 2 - SQL](#Partie-2---sql)

# Partie 1 - Pipeline de données

## Structure du projet

Cette structure fournit un environnement organisé pour l'ingestion, le nettoyage, le traitement, l'exportation et les tests des données, avec une séparation claire entre la couche Raw (données brutes) et la couche Staging (données préparées) permettant de garantir une meilleure gestion des données et une traçabilité accrue du processus de traitement, de l'ingestion initiale jusqu'à l'analyse finale.

```plaintext
.
│   .gitignore
│   conftest.py
│   Dockerfile
│   poetry.lock
│   pyproject.toml
│   pytest.ini
│   README.md
│
├───data
│   ├───Raw
│   │       Src_clinical_trials.csv
│   │       Src_drugs.csv
│   │       Src_pubmed.csv
│   │       Src_pubmed.json
│   │
│   ├───Result
│   │   ├───ad_hoc
│   │   │       most_mentioned_journal.json
│   │   │
│   │   └───link_graph
│   │           drug_mentions_graph.json
│   │
│   └───Staging
│           clinical_trials.csv
│           drugs.csv
│           pubmed.csv
│
├───Sql
│       Req1.sql
│       Req2.sql
│
├───src
│   │   ad_hoc.py
│   │   config.py
│   │   main.py
│   │   __init__.py
│   │
│   ├───layers
│   │       exporter.py
│   │       loader.py
│   │       processor.py
│   │       transformer.py
│   │       __init__.py
│   │
│   └───utils
│           logger.py
│
└───tests
        test_ad_hoc.py
        test_exporter.py
        test_loader.py
        test_processor.py
        test_transformer.py
        __init__.py
```
## Installation

Pour commencer, veuillez confirmer que les exigences suivantes sont remplies pour exécuter le code sur votre ordinateur :
- `Python >= 3.10.9` -  [site officiel](https://www.python.org/downloads/).
- `git` pour cloner le dépôt ou télécharger le dépôt (.zip)
- `poetry` pour le packaging (téléchargé en utilisant `pip install poetry`) 
- `docker` (téléchargé depuis le [site officiel](https://www.docker.com/products/docker-desktop/)).

## Commandes
 Générer le fichier drug_mentions_graph.json
```bash
poetry run main 
```
Générer le fichier most_mentioned_journal.json
```bash
poetry run ad_hoc 
```

Test unitaire
```bash
poetry run pytest -vv tests/
 ```

Construire l'image docker
```bash
docker build -t servier-test-python:latest -f 
 ```
Cette configuration exécutera le code à l'intérieur du conteneur.
```bash
docker run -it servier-test-python:latest 
```

## Production

### A. Déploiement
Pour optimiser la configuration du pipeline de production, il faut envisager ces améliorations :

- Utiliser des variables spécifiques à l'environnement (.env_dev, .env_ppd, .env_prd) pour une meilleure séparation des tests et des environnements.
- Implémenter CI/CD avec des outils comme GitLab CI, Jenkins ou Cloud Build pour rationaliser le déploiement "Développement à Production".
- Choisir une stratégie CI/CD
- Créer un modèle à 3 branches (develop, preprod, prod) avec des déclencheurs sur les merge requests.
- Créer des PR (Pull Requests)
- Exécuter des tests de sécurité, de régression et de bout en bout dans CI/CD avec des rapports.

### B. Orchestration


Orchestration avec Cloud Composer
Pour gérer et planifier l'exécution de ce pipeline en production, Google Cloud Composer (basé sur Apache Airflow) est utilisé pour l'orchestration.

Étapes d'intégration de Cloud Composer :

1.  Définir le DAG (Directed Acyclic Graph) :

    - Créer un fichier DAG dans Airflow pour définir les tâches du pipeline.
    - Chaque tâche (par exemple, extraction de données, transformation) est représentée comme une fonction Python ou un script à appeler avec des opérateurs Airflow (PythonOperator, BashOperator).

2.  Configuration de l'environnement :

    - Dans Google Cloud Console, créer un environnement Cloud Composer avec les ressources et configurations requises (par exemple, type de machine, espace disque).

3. Télécharger le fichier DAG :

   - Placer le fichier DAG dans le dossier dags/ de Cloud Composer pour l'activer.

4. Variables d'environnement :

   - Définir des variables d'environnement dans l'environnement Cloud Composer pour chaque étape du pipeline.

5. Déclencher et surveiller le DAG :

   - Utiliser l'interface utilisateur Airflow pour déclencher le DAG manuellement ou configurer un calendrier pour une exécution périodique.

   - Surveiller la progression des tâches, consulter les journaux et résoudre les problèmes directement depuis l'interface utilisateur Airflow.

Orchestration avec Cloud Workflows :

Une autre option pour l'orchestration est d'utiliser Google Cloud Workflows, qui permet de définir et d'exécuter des workflows complexes en utilisant des services Google Cloud.

Étapes d'intégration de Cloud Workflows :

1. Définir le workflow :

   - Créer un fichier YAML ou JSON pour définir les étapes du workflow.
   - Chaque étape peut appeler un service Google Cloud, exécuter une fonction Cloud ou effectuer des transformations de données.

2. Déployer le workflow :

   - Utiliser la Google Cloud Console ou la CLI gcloud pour déployer le fichier de workflow.

3. Configurer les déclencheurs :

   - Configurer des déclencheurs pour exécuter le workflow automatiquement en réponse à des événements, tels que des modifications de fichiers dans Google Cloud Storage ou des messages dans Pub/Sub.

4. Surveiller et gérer le workflow :

   - Utiliser la Google Cloud Console pour surveiller l'exécution du workflow, consulter les logs et gérer les erreurs.

5. Variables d'environnement :

   - Définir des variables d'environnement dans le fichier de workflow pour stocker des informations sensibles ou spécifiques à l'environnement.

## Comment adapter le code pour le Big Data

Pour adapter le code au Big Data (millions de lignes, téraoctets de données), il existe deux options pour le pipeline ETL :

1. Utiliser pySpark au lieu de pandas pour permettre le calcul distribué :

   - L'adaptation du code devrait être relativement simple en raison des similitudes de syntaxe entre pandas et pySpark.
   - Le code doit s'exécuter sur une machine virtuelle avec des ressources suffisantes (CPU, GPU, mémoire) pour gérer la charge de travail.

2. Alternativement, charger les données dans BigQuery et effectuer les transformations ETL en utilisant SQL :

   - Stocker les fichiers d'entrée dans Google Cloud Storage pour une ingestion transparente dans BigQuery.
   - Utiliser des transformations SQL orchestrées dans un projet dbt versionné pour gérer et automatiser le processus.
   - Les fichiers de sortie doivent être enregistrés dans Google Cloud Storage, avec des politiques de cycle de vie configurées pour éviter la perte accidentelle de données.


 # Partie 2 - SQL 


- Requête 1 : Ventes quotidiennes entre le 1er janvier 2019 et le 31 décembre 2019 
```sql
SELECT date, SUM(prod_qty) AS ventes

FROM `some_project.servier.TRANSACTION`

WHERE date >= DATE(2019, 1, 1) AND date <= DATE(2019, 1, 31)

GROUP BY date
```


- Requête 2 : Ventes de décoration et de meubles par client, entre le 1er janvier 2019 et le 31 décembre 2019 
```sql
SELECT

  client_id,

  SUM(CASE WHEN product_type = 'MEUBLE' THEN prod_qty ELSE 0 END) AS ventes_meuble,

  SUM(CASE WHEN product_type = 'DECO' THEN prod_qty ELSE 0 END) AS ventes_deco

FROM `some_project.servier. TRANSACTION  ` AS transactions

LEFT JOIN `some_project.servier. PRODUCT_NOMENCLATURE ` AS product_nomenclature ON transactions.prod_id = product_nomenclature.product_id

WHERE transactions.date >= DATE(2019, 1, 1) AND transactions.date <= DATE(2019, 1, 31)

GROUP BY client_id
```