FROM python:3.9-slim

WORKDIR /app

# Installation de la librairie Kafka
RUN pip install kafka-python

# Copie du code
COPY src/ ./src/

# Commande par défaut (sera surchargée par le mode interactif)
CMD ["python", "src/player_manager.py"]
