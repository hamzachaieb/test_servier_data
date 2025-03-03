import os
import sys
from typing import Tuple
import pandas as pd
import json

from src import config
from src.layers.loader import load_csv, load_json, load_text
from src.layers.transformer import (
    correct_json_text,
    standardize_date_format,
    convert_id_to_string,
    sanitize_title_text,
    remove_rows_with_empty_titles_or_journals,
    remove_duplicate_ids_and_reindex
)
from src.layers.processor import build_drug_mentions_graph
from src.layers.exporter import export_to_json
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_and_transform() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Charge et nettoie les données sources.
    
    Étapes :
      1. Nettoie le fichier JSON brut de PubMed.
      2. Charge les fichiers CSV sources.
      3. Combine les données PubMed issues du CSV et du JSON.
      4. Applique les opérations de transformation et de nettoyage.
      5. Sauvegarde les fichiers nettoyés dans le dossier de préparation.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: (drugs_df, pubmed_df, clinical_trials_df)
    """
    # Traitement du JSON PubMed
    try:
        # Tente de charger le JSON nettoyé depuis le fichier source
        pubmed_json_df: pd.DataFrame = load_json(config.SRC_PUBMED_JSON_FILE_PATH)
    except json.JSONDecodeError as e:
        logger.info(f"Erreur JSON détectée : {e}. Tentative de nettoyage en mode texte...")
        try:
            pubmed_text: str = load_text(config.SRC_PUBMED_JSON_FILE_PATH)
            pubmed_json_df = correct_json_text(pubmed_text)
        except Exception as e2:
            logger.error(f"Erreur lors de la correction du JSON PubMed: {e2}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur lors du chargement du JSON PubMed: {e}")
        sys.exit(1)

    # Chargement des fichiers CSV
    try:
        logger.info("Chargement des fichiers CSV sources...")
        drugs_df: pd.DataFrame = load_csv(config.SRC_DRUGS_FILE_PATH)
        pubmed_csv_df: pd.DataFrame = load_csv(config.SRC_PUBMED_FILE_PATH)
        clinical_trials_df: pd.DataFrame = load_csv(config.SRC_CLINICAL_TRIALS_FILE_PATH)
    except Exception as e:
        logger.error(f"Erreur lors du chargement des fichiers CSV: {e}")
        sys.exit(1)

    # Combinaison des données PubMed
    try:
        logger.info("Combinaison des données PubMed issues du CSV et du JSON...")
        pubmed_df: pd.DataFrame = pd.concat([pubmed_csv_df, pubmed_json_df], ignore_index=True)
    except Exception as e:
        logger.error(f"Erreur lors de la combinaison des données PubMed: {e}")
        sys.exit(1)

    # Transformation et nettoyage
    try:
        logger.info("Transformation et nettoyage des données...")
        # Traitement du fichier drugs
        drugs_df = convert_id_to_string(drugs_df, 'atccode')
        drugs_df = remove_duplicate_ids_and_reindex(drugs_df, 'atccode')
        drugs_df.to_csv(config.DRUGS_FILE_PATH, index=False)
        
        # Standardisation des dates
        pubmed_df = standardize_date_format(pubmed_df, 'date')
        clinical_trials_df = standardize_date_format(clinical_trials_df, 'date')
        
        # Nettoyage des titres et journaux
        pubmed_df = sanitize_title_text(pubmed_df, 'title')
        clinical_trials_df = sanitize_title_text(clinical_trials_df, 'scientific_title')
        pubmed_df = remove_rows_with_empty_titles_or_journals(pubmed_df, 'title', 'journal')
        clinical_trials_df = remove_rows_with_empty_titles_or_journals(clinical_trials_df, 'scientific_title', 'journal')
        
        # Suppression des doublons sur les IDs
        pubmed_df = remove_duplicate_ids_and_reindex(pubmed_df, 'id')
        clinical_trials_df = remove_duplicate_ids_and_reindex(clinical_trials_df, 'id')
        
        # Sauvegarde des fichiers nettoyés
        pubmed_df.to_csv(config.PUBMED_FILE_PATH, index=False)
        clinical_trials_df.to_csv(config.CLINICAL_TRIALS_FILE_PATH, index=False)
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage des données CSV: {e}")
        sys.exit(1)
    
    return drugs_df, pubmed_df, clinical_trials_df

def build_and_export_graph() -> None:
    """
    Construit le graphe de mentions des médicaments et exporte le résultat en JSON.
    """
    drugs_df, pubmed_df, clinical_trials_df = load_and_transform()
    
    logger.info("Construction du graphe de mentions de médicaments...")
    try:
        graph_data = build_drug_mentions_graph(drugs_df, pubmed_df, clinical_trials_df)
    except Exception as e:
        logger.error(f"Erreur lors de la construction du graphe: {e}")
        sys.exit(1)
    
    if not graph_data:
        logger.error("Aucun contenu dans le graphe à exporter.")
        sys.exit(1)
    
    try:
        export_to_json(graph_data, config.OUTPUT_JSON_PATH)
        logger.info(f"Graph exporté avec succès dans {config.OUTPUT_JSON_PATH}")
    except Exception as e:
        logger.error(f"Erreur lors de l'exportation du graphe: {e}")
        sys.exit(1)


def main() -> None:
    """
    Point d'entrée de la pipeline ETL.
    """
    logger.info("Début de la pipeline ETL...")
    build_and_export_graph()
    logger.info("Pipeline ETL terminée avec succès.")

if __name__ == "__main__":
    # Création du dossier de sortie si nécessaire
    os.makedirs(config.LINK_GRAPH_DIR, exist_ok=True)
    main()
