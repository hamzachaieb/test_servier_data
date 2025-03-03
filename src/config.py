import os

RAW_DATA_DIR = os.path.join('data', 'Raw')
STAGING_DATA_DIR = os.path.join('data', 'Staging')
RESULT_DIR = os.path.join('data', 'Result')
LINK_GRAPH_DIR = os.path.join(RESULT_DIR, 'link_graph')
AD_HOC_DIR = os.path.join(RESULT_DIR, 'ad_hoc')

# Fichiers sources
SRC_DRUGS_FILE_PATH = os.path.join(RAW_DATA_DIR, 'Src_drugs.csv')
SRC_PUBMED_FILE_PATH = os.path.join(RAW_DATA_DIR, 'Src_pubmed.csv')
SRC_PUBMED_JSON_FILE_PATH = os.path.join(RAW_DATA_DIR, 'Src_pubmed.json')
SRC_CLINICAL_TRIALS_FILE_PATH = os.path.join(RAW_DATA_DIR, 'Src_clinical_trials.csv')

# Fichiers après nettoyage
DRUGS_FILE_PATH = os.path.join(STAGING_DATA_DIR, 'drugs.csv')
PUBMED_FILE_PATH = os.path.join(STAGING_DATA_DIR, 'pubmed.csv')
PUBMED_JSON_FILE_PATH = os.path.join(STAGING_DATA_DIR, 'pubmed.json')
CLINICAL_TRIALS_FILE_PATH = os.path.join(STAGING_DATA_DIR, 'clinical_trials.csv')

# Dossiers de sortie
OUTPUT_JSON_PATH = os.path.join(LINK_GRAPH_DIR, 'drug_mentions_graph.json')
AD_HOC_OUTPUT_PATH = os.path.join(AD_HOC_DIR, 'most_mentioned_journal.json')

