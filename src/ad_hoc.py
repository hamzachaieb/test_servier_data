import json
import sys
import pandas as pd
from typing import Dict, Any
from src import config
from src.layers.loader import load_json
from src.layers.exporter import export_to_json
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_graph_data() -> pd.DataFrame:
    """
    Charge les données du graphe de mentions à partir du fichier OUTPUT_JSON_PATH.
    En cas d'erreur "ValueError: All arrays must be of the same length", tente de reconstruire
    un DataFrame en concaténant les données issues du JSON.
    
    Returns:
        pd.DataFrame: DataFrame contenant les données du graphe.
    """
    try:
        df: pd.DataFrame = load_json(config.OUTPUT_JSON_PATH)
        logger.info("Données du graphe chargées depuis OUTPUT_JSON_PATH.")
        return df
    except ValueError as e:
        logger.warning(f"Erreur de chargement par load_json détectée : {e}. "
                        "Tentative de reconstruction via concaténation...")
        with open(config.OUTPUT_JSON_PATH, 'r') as file:
            data = json.load(file)
        df = pd.concat(
        [pd.DataFrame(value).assign(categorie=key) for key, value in data.items()],
        ignore_index=True
        )
        logger.info("Données du graphe chargées avec succès via concaténation.")
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement JSON depuis OUTPUT_JSON_PATH : {e}")
        sys.exit(1)


def compute_most_mentioned_journal(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcule le journal qui mentionne le plus de médicaments distincts.

    Args:
        df (pd.DataFrame): DataFrame contenant au moins les colonnes 'journal' et 'categorie'.

    Returns:
        Dict[str, Any]: Dictionnaire avec les clés 'journal' et 'mentions'.
    """
    if 'journal' not in df.columns or 'categorie' not in df.columns:
        logger.error("Les colonnes 'journal' et/ou 'categorie' sont absentes du DataFrame.")
        sys.exit(1)
    try:
        journal_counts = df.groupby("journal")["categorie"].nunique()
        most_mentioned = journal_counts.idxmax()
        count_mentions = int(journal_counts.max())
        return {"journal": most_mentioned, "mentions": count_mentions}
    except Exception as e:
        logger.error(f"Erreur lors du calcul des mentions par journal : {e}")
        sys.exit(1)

def export_most_mentioned_journal() -> Dict[str, Any]:
    """
    Calcule le journal le plus mentionné à partir des données du graphe et exporte
    le résultat en JSON dans le chemin défini par config.AD_HOC_OUTPUT_PATH.

    Returns:
        Dict[str, Any]: Le résultat du calcul sous forme de dictionnaire.
    """
    df = load_graph_data()
    result = compute_most_mentioned_journal(df)
    
    logger.warning("\nLe journal le plus mentionné :\n" + json.dumps(result, indent=4))
    
    try:
        export_to_json(result, config.AD_HOC_OUTPUT_PATH)
        logger.info(f"Le fichier JSON du journal le plus mentionné a été exporté dans {config.AD_HOC_OUTPUT_PATH}.")
        return result
    except Exception as e:
        logger.error(f"Erreur lors de l'exportation du résultat ad hoc : {e}")
        sys.exit(1)

if __name__ == "__main__":
    export_most_mentioned_journal()
