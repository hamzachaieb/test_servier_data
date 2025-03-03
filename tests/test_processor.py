import pandas as pd
from src.layers.processor import build_drug_mentions_graph

def test_build_drug_mentions_graph():
    # Données d'exemple pour les médicaments
    drugs_data = {'drug': ['Aspirin', 'Ibuprofen']}
    drugs_df = pd.DataFrame(drugs_data)
    
    # Données pour PubMed
    pubmed_data = {
        'id': ['1', '2'],
        'title': ['Aspirin reduces pain', 'An unrelated article'],
        'journal': ['Journal A', 'Journal B'],
        'date': ['2020-01-01', '2020-02-01']
    }
    pubmed_df = pd.DataFrame(pubmed_data)
    
    # Données pour essais cliniques
    clinical_data = {
        'id': ['CT1', 'CT2'],
        'scientific_title': ['Study on Ibuprofen effectiveness', 'Another study'],
        'journal': ['Journal C', 'Journal D'],
        'date': ['2020-03-01', '2020-04-01']
    }
    clinical_trials_df = pd.DataFrame(clinical_data)
    
    graph = build_drug_mentions_graph(drugs_df, pubmed_df, clinical_trials_df)
    
    # Aspirin devrait avoir une mention dans PubMed
    assert 'Aspirin' in graph
    assert len(graph['Aspirin']) == 1
    # Ibuprofen devrait avoir une mention dans les essais cliniques
    assert 'Ibuprofen' in graph
    assert len(graph['Ibuprofen']) == 1
