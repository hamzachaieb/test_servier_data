import pandas as pd
import json
import re
from typing import Any
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

def correct_json_text(raw_text: str) -> pd.DataFrame:
    """
    Corrige le texte brut d'un fichier JSON en ajoutant des guillemets manquants
    et en retirant les virgules superflues, puis retourne un DataFrame.
    
    Args:
        raw_text (str): Texte brut du fichier JSON.
        
    Returns:
        pd.DataFrame: DataFrame construit à partir du JSON corrigé.
    """
    try:
        corrected_text: str = re.sub(r'(?<!")(\b\w+\b)(?!")(?=\s*:)', r'"\1"', raw_text)
        corrected_text = re.sub(r',\s*([\]}])', r'\1', corrected_text)
        if not corrected_text.strip().startswith("["):
            corrected_text = f"[{corrected_text.strip()}]"
        try:
            corrected_data: Any = json.loads(corrected_text)
            # Filtrer les objets dont la clé 'id' n'est pas vide
            cleaned_data = [item for item in corrected_data if item.get("id") != ""]
            logger.info("JSON corrigé et converti en DataFrame avec succès.")
            return pd.DataFrame(cleaned_data)
        except json.JSONDecodeError as inner_e:
            logger.error(f"Erreur persistante après correction : {inner_e}")
            raise inner_e
    except Exception as ex:
        logger.error(f"Une erreur inattendue est survenue lors de la correction du JSON : {ex}")
        raise ex


def standardize_date_format(df: pd.DataFrame, date_column_name: str) -> pd.DataFrame:
    """
    Convertit dynamiquement la colonne de dates d'un DataFrame au format 'YYYY-MM-DD'
    en essayant plusieurs formats possibles selon le contenu de la date.

    Les formats testés sont :
      - "%d/%m/%Y" si la date contient '/'
      - "%Y-%m-%d" puis "%d-%m-%Y" si la date contient '-'
      - "%d %B %Y" si la date contient un espace

    En cas d'échec, la date sera remplacée par une chaîne vide.

    Args:
        df (pd.DataFrame): DataFrame contenant la colonne de dates.
        date_column_name (str): Nom de la colonne à convertir.

    Returns:
        pd.DataFrame: DataFrame avec la colonne de dates formatée en 'YYYY-MM-DD'.
    """
    if date_column_name not in df.columns:
        logger.warning(f"La colonne {date_column_name} n'existe pas dans le DataFrame.")
        return df

    def dynamic_clean_date(date_str: str) -> str:
        if date_str is None or pd.isna(date_str):
            return ""
        try:
            if '/' in date_str:
                # Format attendu: "jour/mois/année"
                return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
            elif '-' in date_str:
                # On essaie d'abord le format "année-mois-jour"
                try:
                    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
                except ValueError:
                    # Sinon, on essaie "jour-mois-année"
                    return datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
            elif ' ' in date_str:
                # Format attendu: "jour Mois année" (ex: "1 January 2020")
                return datetime.strptime(date_str, "%d %B %Y").strftime("%Y-%m-%d")
            else:
                raise ValueError("Format de date non reconnu")
        except ValueError as e:
            logger.error(f"Erreur lors du parsing de la date '{date_str}': {e}")
            return ""

    df[date_column_name] = df[date_column_name].apply(dynamic_clean_date)
    return df


def convert_id_to_string(df: pd.DataFrame, id_column_name: str) -> pd.DataFrame:
    """
    Convertit les valeurs d'une colonne d'ID en chaînes de caractères.
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne d'ID.
        id_column_name (str): Nom de la colonne d'ID.
    
    Returns:
        pd.DataFrame: DataFrame avec la colonne d'ID convertie en str.
    """
    if id_column_name in df.columns:
        df[id_column_name] = df[id_column_name].astype(str)
    else:
        logger.warning(f"La colonne {id_column_name} n'existe pas dans le DataFrame.")
    return df


def sanitize_title_text(df: pd.DataFrame, title_column_name: str) -> pd.DataFrame:
    """
    Nettoie une colonne de titres dans un DataFrame : supprime les caractères non-ASCII,
    les ponctuations indésirables et normalise la casse.
    
    Args:
        df (pd.DataFrame): DataFrame contenant la colonne de titres.
        title_column_name (str): Nom de la colonne à nettoyer.
    
    Returns:
        pd.DataFrame: DataFrame avec la colonne nettoyée.
    """
    if title_column_name in df.columns:
        df[title_column_name] = df[title_column_name].str.encode('ascii', 'ignore').str.decode('utf-8')
        df[title_column_name] = df[title_column_name].str.replace(r'[^\w\s-]', '', regex=True)
        df[title_column_name] = df[title_column_name].str.title().str.strip().str.replace(r'\s+', ' ', regex=True)
    else:
        logger.warning(f"La colonne {title_column_name} n'existe pas dans le DataFrame.")
    return df

def remove_rows_with_empty_titles_or_journals(df: pd.DataFrame, title_column_name: str, journal_column_name: str) -> pd.DataFrame:
    """
    Supprime les lignes d'un DataFrame où les colonnes spécifiées de titre ou de journal sont vides.
    
    Args:
        df (pd.DataFrame): DataFrame à filtrer.
        title_column_name (str): Nom de la colonne de titre.
        journal_column_name (str): Nom de la colonne de journal.
    
    Returns:
        pd.DataFrame: DataFrame filtré.
    """
    return df.dropna(subset=[title_column_name, journal_column_name])

def remove_duplicate_ids_and_reindex(df: pd.DataFrame, id_column_name: str) -> pd.DataFrame:
    """
    Supprime les doublons dans une colonne d'ID et réindexe le DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame à traiter.
        id_column_name (str): Nom de la colonne d'ID.
    
    Returns:
        pd.DataFrame: DataFrame sans doublons et réindexé.
    """
    return df.drop_duplicates(subset=[id_column_name]).reset_index(drop=True)