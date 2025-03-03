import json
import os
from typing import Any, Dict
from src.utils.logger import get_logger

logger = get_logger(__name__)

def export_to_json(data: Dict[Any, Any], output_path: str) -> None:
    """
    Exporte un dictionnaire dans un fichier JSON formaté.

    Args:
        data (Dict[Any, Any]): Données à exporter.
        output_path (str): Chemin complet du fichier JSON de sortie.
    """
    if not data:
        logger.error("Erreur : Aucun contenu à exporter dans le fichier JSON.")
        return

    try:
        # Assurer que le dossier de destination existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
        logger.info(f"Exportation réussie du fichier JSON dans {output_path}.")
    except Exception as e:
        logger.error(f"Erreur lors de l'exportation du fichier JSON: {e}")
