#!/bin/bash
set -e   # arrêter le script si une commande échoue

echo "=== Lancement du projet UA3 Flights ==="

# 1) S'assurer que le dossier HDFS existe
hdfs dfs -mkdir -p /user/root/projet_ua3

# 2) Copier flights.csv dans HDFS si le fichier n'existe pas déjà
if ! hdfs dfs -test -e /user/root/projet_ua3/flights.csv ; then
  echo "[INFO] Copie de /root/flights.csv vers HDFS..."
  hdfs dfs -put -f /root/flights.csv /user/root/projet_ua3/flights.csv
else
  echo "[INFO] flights.csv déjà présent dans HDFS, on ne recopie pas."
fi

# 3) Supprimer complètement l'ancien répertoire de sortie
echo "[INFO] Nettoyage de /user/root/projet_ua3/output (si présent)..."
hdfs dfs -rm -r -f /user/root/projet_ua3/output || true

# 4) Lancer le script Spark principal
echo "[INFO] Lancement de Spark (flights_project.py)..."
spark-submit --master local[*] /root/flights_project.py

echo "=== Fin du projet UA3 Flights ==="
