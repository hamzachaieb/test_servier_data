import os
import json
from src.ad_hoc import export_most_mentioned_journal
from src import config

def test_export_most_mentioned_journal():
    # Définir des chemins de test dans le dossier tests
    test_input_path = "tests/test_drug_mentions_graph.json"
    test_output_path = "tests/test_ad_hoc_output.json"

    # Préparer des données factices pour simuler le graphe de mentions
    test_data = {
        "DrugA": [{"journal": "Journal1"}, {"journal": "Journal2"}],
        "DrugB": [{"journal": "Journal1"}]
    }

    # Écrire ces données dans le fichier d'entrée de test
    with open(test_input_path, "w", encoding="utf-8") as f:
        json.dump(test_data, f, indent=4)

    # Redéfinir les chemins dans la configuration pour pointer vers nos fichiers de test
    config.OUTPUT_JSON_PATH = test_input_path
    config.AD_HOC_OUTPUT_PATH = test_output_path

    # Appeler la fonction ad hoc pour générer le résultat
    result = export_most_mentioned_journal()

    # Dans cet exemple, "Journal1" est mentionné par DrugA et DrugB (2 médicaments distincts)
    expected = {"journal": "Journal1", "mentions": 2}
    assert result == expected

    # Vérifier que le fichier d'export a été créé et contient le résultat attendu
    assert os.path.exists(test_output_path)
    with open(test_output_path, "r", encoding="utf-8") as f:
        exported_data = json.load(f)
    assert exported_data == expected

    # Nettoyer les fichiers de test
    os.remove(test_input_path)
    os.remove(test_output_path)
