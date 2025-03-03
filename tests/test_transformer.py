import pandas as pd
from src.layers.transformer import (
    correct_json_text,
    standardize_date_format,
    convert_id_to_string,
    sanitize_title_text,
    remove_rows_with_empty_titles_or_journals,
    remove_duplicate_ids_and_reindex
)

def test_correct_json_text():
    # Exemple de texte JSON brut avec des clés non citées
    raw_text = '{id: "1", name: "Test"}'
    df = correct_json_text(raw_text)
    # On s'attend à obtenir un DataFrame avec les colonnes 'id' et 'name'
    assert isinstance(df, pd.DataFrame)
    assert "id" in df.columns
    assert df.iloc[0]["id"] == "1"

def test_standardize_date_format():
    data = {'date': ['01/02/2020', '2020-03-04', '15 April 2021', None, '']}
    df = pd.DataFrame(data)
    df_clean = standardize_date_format(df, 'date')
    expected = ['2020-02-01', '2020-03-04', '2021-04-15', '', '']
    assert df_clean['date'].tolist() == expected

def test_convert_id_to_string():
    data = {'id': [123, 456, None]}
    df = pd.DataFrame(data)
    df_converted = convert_id_to_string(df, 'id')
    # Vérifier que toutes les valeurs sont des chaînes
    for value in df_converted['id']:
        assert isinstance(value, str)

def test_sanitize_title_text():
    data = {'title': ['hello! world?', 'Cafe au lait', None]}
    df = pd.DataFrame(data)
    df_sanitized = sanitize_title_text(df, 'title')
    # "hello! world?" -> "Hello World" et "Café au lait" -> "Cafe Au Lait"
    assert df_sanitized['title'].iloc[0] == 'Hello World'
    assert df_sanitized['title'].iloc[1] == 'Cafe Au Lait'

def test_remove_rows_with_empty_titles_or_journals():
    data = {
        'title': ['Test Title', None, 'Another Title'],
        'journal': ['Journal A', 'Journal B', None]
    }
    df = pd.DataFrame(data)
    df_filtered = remove_rows_with_empty_titles_or_journals(df, 'title', 'journal')
    # Seules les lignes avec titre ET journal non vides doivent être conservées
    assert len(df_filtered) == 1
    assert df_filtered.iloc[0]['title'] == 'Test Title'

def test_remove_duplicate_ids_and_reindex():
    data = {'id': ['1', '1', '2'], 'value': [10, 10, 20]}
    df = pd.DataFrame(data)
    df_clean = remove_duplicate_ids_and_reindex(df, 'id')
    # On s'attend à obtenir deux lignes
    assert len(df_clean) == 2
