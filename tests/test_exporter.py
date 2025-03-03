import os
import json
import tempfile
from src.layers.exporter import export_to_json

def test_export_to_json():
    data = {"key": "value", "numbers": [1, 2, 3]}
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
        tmp_name = tmp.name
    try:
        export_to_json(data, tmp_name)
        with open(tmp_name, 'r', encoding='utf-8') as f:
            exported_data = json.load(f)
        assert exported_data == data
    finally:
        os.remove(tmp_name)
