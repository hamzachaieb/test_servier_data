# src/utils/logger.py

import logging
import sys

def get_logger(name: str = __name__) -> logging.Logger:
    """
    Retourne un logger configuré pour l'affichage sur la console.

    Args:
        name (str): Le nom du logger. Par défaut, le nom du module courant.

    Returns:
        logging.Logger: Un logger configuré avec un niveau de log DEBUG.
    """
    logger = logging.getLogger(name)
    
    # Pour éviter d'ajouter plusieurs handlers si le logger est déjà configuré
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        
        # Crée un handler qui affiche les logs sur la console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        
        # Définit un format pour les messages de log
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        # Ajoute le handler au logger
        logger.addHandler(console_handler)
    
    return logger
