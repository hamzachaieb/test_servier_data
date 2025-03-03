import os
import json
import pandas as pd
import tempfile
from src.layers.loader import load_csv, load_json, load_text

def test_load_csv():
    # Créer un fichier CSV temporaire
    data = "col1,col2\n1,2\n3,4\n"
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp:
        tmp.write(data)
        tmp_name = tmp.name
    try:
        df = load_csv(tmp_name)
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["col1", "col2"]
    finally:
        os.remove(tmp_name)

def test_load_json_and_load_text():
    # Créer un fichier JSON temporaire
    data = {"key": "value", "numbers": [1, 2, 3]}
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
        json.dump(data, tmp)
        tmp_name = tmp.name
    try:
        df = load_json(tmp_name)
        # On s'attend à obtenir un DataFrame contenant les clés du dictionnaire
        assert isinstance(df, pd.DataFrame)
        text = load_text(tmp_name)
        assert isinstance(text, str)
        assert "value" in text
    finally:
        os.remove(tmp_name)
