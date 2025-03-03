import pandas as pd
import json

def load_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def load_json(file_path: str) -> pd.DataFrame:
    with open(file_path, 'r') as file:
        data = json.load(file)
    return pd.DataFrame(data)

def load_text(file_path: str) -> str:
    with open(file_path, 'r') as file:
        raw_text = file.read()
    return raw_text