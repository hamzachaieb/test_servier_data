# Utiliser une image Python comme base
FROM python:3.9-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Installer Poetry
RUN pip install poetry

# Copier les fichiers pyproject.toml et poetry.lock pour installer les dépendances
COPY pyproject.toml poetry.lock ./

# Installer les dépendances sans créer d'environnement virtuel dans Docker
RUN poetry config virtualenvs.create false && poetry install --no-dev

# Copier tous les fichiers de l'application dans le conteneur
COPY . .

# Créer le dossier de sortie si nécessaire
RUN mkdir -p data/Result/link_graph data/Result/ad_hoc

# Définir le chemin PYTHONPATH pour le conteneur
ENV PYTHONPATH=/app

# Définir la commande par défaut pour exécuter le script main.py
CMD ["poetry", "run", "python", "src/main.py"]
