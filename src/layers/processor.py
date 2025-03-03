import pandas as pd
import re
from typing import Dict, List, Any
from src.utils.logger import get_logger

logger = get_logger(__name__)

def build_drug_mentions_graph(
    drugs_df: pd.DataFrame, 
    pubmed_df: pd.DataFrame, 
    clinical_trials_df: pd.DataFrame
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Construit un graphe de mentions des médicaments à partir des DataFrames fournis.

    Pour chaque médicament présent dans `drugs_df`, recherche dans les DataFrames
    `pubmed_df` et `clinical_trials_df` les articles dont le titre (ou titre scientifique)
    contient le nom du médicament (en ignorant la casse) et construit un dictionnaire
    de mentions.

    Args:
        drugs_df (pd.DataFrame): DataFrame contenant une colonne 'drug'.
        pubmed_df (pd.DataFrame): DataFrame contenant les articles PubMed avec les colonnes 'title', 'id', 'journal' et 'date'.
        clinical_trials_df (pd.DataFrame): DataFrame contenant les essais cliniques avec les colonnes 'scientific_title', 'id', 'journal' et 'date'.

    Returns:
        Dict[str, List[Dict[str, Any]]]: Dictionnaire où chaque clé est le nom d'un médicament et la valeur est
                                          une liste de dictionnaires décrivant les mentions (source, id, title, journal, date).
    """
    graph_data: Dict[str, List[Dict[str, Any]]] = {}

    for _, drug_row in drugs_df.iterrows():
        drug: str = str(drug_row['drug'])
        mentions: List[Dict[str, Any]] = []
        # Utilisation de re.escape pour éviter que des caractères spéciaux dans le nom du médicament ne perturbent le pattern.
        drug_pattern = rf'\b{re.escape(drug)}\b'

        # Recherche dans les articles PubMed
        for _, row in pubmed_df.iterrows():
            title = row.get('title', '')
            date = row.get('date', '')
            date = date if pd.notna(date) else ""
            if isinstance(title, str) and re.search(drug_pattern, title, re.IGNORECASE):
                mentions.append({
                    'source': 'pubmed',
                    'id': row.get('id'),
                    'title': title,
                    'journal': row.get('journal', ''),
                    'date': date
                })

        # Recherche dans les essais cliniques
        for _, row in clinical_trials_df.iterrows():
            scientific_title = row.get('scientific_title', '')
            date = row.get('date', '')
            date = date if pd.notna(date) else ""
            if isinstance(scientific_title, str) and re.search(drug_pattern, scientific_title, re.IGNORECASE):
                mentions.append({
                    'source': 'clinical_trials',
                    'id': row.get('id'),
                    'title': scientific_title,
                    'journal': row.get('journal', ''),
                    'date': date
                })

        if mentions:
            graph_data[drug] = mentions

    return graph_data
