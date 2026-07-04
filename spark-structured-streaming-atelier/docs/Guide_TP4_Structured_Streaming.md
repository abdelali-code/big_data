# TP 4 : Analyse en temps quasi réel des mesures de capteurs avec PySpark Structured Streaming et HDFS

*Big Data 2026 — ENSET Mohammedia*

## 1. Titre du TP

Traitement de flux de mesures de capteurs avec **PySpark Structured Streaming** à partir de
fichiers déposés progressivement dans **HDFS**.

## 2. Problématique

Dans un système de supervision, des fichiers contenant les mesures de plusieurs capteurs sont
déposés régulièrement dans un dossier HDFS. L'entreprise souhaite exploiter ces données dès
leur arrivée afin de :

- surveiller l'évolution des mesures ;
- calculer des indicateurs en continu ;
- détecter rapidement les valeurs anormales ;
- afficher les résultats en temps quasi réel.

L'objectif est donc de mettre en place une application PySpark Structured Streaming capable de
lire automatiquement les nouveaux fichiers CSV déposés dans HDFS et de traiter ces données en
flux quasi temps réel.

## 3. Objectifs pédagogiques

À la fin de ce TP, l'étudiant doit être capable de :

- comprendre le principe de Spark Structured Streaming ;
- utiliser HDFS comme source de streaming ;
- développer une application streaming avec PySpark ;
- définir un schéma explicite pour lire des fichiers CSV ;
- réaliser des traitements et des agrégations sur un flux ;
- configurer un checkpoint ;
- détecter des valeurs anormales ;
- exécuter une application PySpark avec `spark-submit`.

## 4. Prérequis

- un cluster HDFS fonctionnel ;
- un cluster Spark Standalone fonctionnel ;
- Docker si l'environnement est conteneurisé ;
- Python installé ;
- PySpark installé ou disponible dans le conteneur Spark ;
- un répertoire HDFS pour les fichiers source ;
- un répertoire HDFS pour les checkpoints.

## 5. Données à traiter

Structure des fichiers CSV :

```
id,timestamp,capteur,valeur,unite
1,2026-03-30 09:00:00,CAPTEUR_TEMP_1,22.5,C
2,2026-03-30 09:00:05,CAPTEUR_TEMP_2,24.1,C
3,2026-03-30 09:00:10,CAPTEUR_HUM_1,60.3,%
4,2026-03-30 09:00:15,CAPTEUR_TEMP_1,23.2,C
```

| Colonne | Description |
|---|---|
| `id` | Identifiant de la mesure |
| `timestamp` | Date et heure de la mesure |
| `capteur` | Nom du capteur |
| `valeur` | Valeur mesurée |
| `unite` | Unité de mesure |

## 6. Travail demandé

1. Créer dans HDFS un dossier qui servira de source de streaming.
2. Créer un dossier HDFS pour les checkpoints.
3. Préparer plusieurs fichiers CSV contenant des mesures de capteurs.
4. Développer une application PySpark Structured Streaming.
5. Lire les nouveaux fichiers CSV depuis HDFS.
6. Utiliser un schéma explicite.
7. Calculer en streaming : la moyenne, le minimum, le maximum et le nombre de mesures par
   capteur.
8. Identifier les mesures anormales dépassant un seuil donné.
9. Exécuter l'application.
10. Déposer progressivement les fichiers dans HDFS.
11. Observer les résultats générés après chaque nouveau fichier.

## 7. Préparation de HDFS

```bash
hdfs dfs -mkdir -p /streaming/capteurs
hdfs dfs -mkdir -p /streaming/checkpoints/capteurs_stats
hdfs dfs -mkdir -p /streaming/checkpoints/capteurs_alertes

hdfs dfs -ls /streaming
hdfs dfs -ls /streaming/checkpoints
```

Pour relancer le TP depuis le début :

```bash
hdfs dfs -rm -r /streaming/capteurs/*
hdfs dfs -rm -r /streaming/checkpoints/capteurs_stats/*
hdfs dfs -rm -r /streaming/checkpoints/capteurs_alertes/*
```

## 8. Structure du projet

```
tp-pyspark-streaming-capteurs/
    app.py
    data/
        capteurs_1.csv
        capteurs_2.csv
        capteurs_3.csv
```

## 9. Fichiers CSV

### 9.1. `capteurs_1.csv`

```
id,timestamp,capteur,valeur,unite
1,2026-03-30 09:00:00,CAPTEUR_TEMP_1,22.5,C
2,2026-03-30 09:00:05,CAPTEUR_TEMP_2,24.1,C
3,2026-03-30 09:00:10,CAPTEUR_HUM_1,60.3,%
4,2026-03-30 09:00:15,CAPTEUR_TEMP_1,23.2,C
```

### 9.2. `capteurs_2.csv`

```
id,timestamp,capteur,valeur,unite
5,2026-03-30 09:01:00,CAPTEUR_TEMP_1,25.4,C
6,2026-03-30 09:01:05,CAPTEUR_TEMP_2,26.8,C
7,2026-03-30 09:01:10,CAPTEUR_HUM_1,65.1,%
8,2026-03-30 09:01:15,CAPTEUR_TEMP_1,40.7,C
```

### 9.3. `capteurs_3.csv`

```
id,timestamp,capteur,valeur,unite
9,2026-03-30 09:02:00,CAPTEUR_TEMP_2,27.2,C
10,2026-03-30 09:02:05,CAPTEUR_HUM_1,70.6,%
11,2026-03-30 09:02:10,CAPTEUR_TEMP_1,21.9,C
12,2026-03-30 09:02:15,CAPTEUR_TEMP_2,45.3,C
```

Dans cet exemple, les valeurs `40.7` et `45.3` peuvent être considérées comme anormales si le
seuil est fixé à `35`.

## 10. Code complet de l'application PySpark

Voir [`../tp-pyspark-streaming-capteurs/app.py`](../tp-pyspark-streaming-capteurs/app.py) —
il reproduit exactement le code du sujet (schéma explicite, lecture du flux, agrégations,
détection d'anomalies, deux `writeStream` en mode `complete`/`append`), avec en plus un mode
`--local` pour tester sans HDFS/Docker.

## 11. Explication du code

### 11.1. Création de la session Spark

```python
spark = SparkSession.builder \
    .appName("TP PySpark Structured Streaming - Capteurs HDFS") \
    .getOrCreate()
```

Cette instruction crée une session Spark. Elle représente le point d'entrée principal pour
utiliser PySpark.

### 11.2. Définition du schéma

```python
schema_capteurs = StructType([
    StructField("id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("capteur", StringType(), True),
    StructField("valeur", DoubleType(), True),
    StructField("unite", StringType(), True)
])
```

En Structured Streaming, il est recommandé de définir un schéma explicite. Spark doit
connaître la structure des fichiers avant de démarrer le flux.

### 11.3. Lecture du flux depuis HDFS

```python
df_stream = spark.readStream \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .schema(schema_capteurs) \
    .csv(source_path)
```

Cette partie permet à Spark de surveiller le dossier HDFS `/streaming/capteurs`. Chaque nouveau
fichier CSV déposé dans ce dossier sera automatiquement traité. L'option
`maxFilesPerTrigger=1` permet de traiter un fichier à chaque micro-batch, pour mieux observer
les résultats dans le cadre d'un TP.

### 11.4. Calcul des statistiques

```python
stats_capteurs = df_stream.groupBy("capteur") \
    .agg(
        avg("valeur").alias("moyenne_valeur"),
        min("valeur").alias("valeur_min"),
        max("valeur").alias("valeur_max"),
        count("*").alias("nombre_mesures")
    )
```

Calcule, pour chaque capteur : la moyenne, la valeur minimale, la valeur maximale et le nombre
total de mesures.

### 11.5. Détection des valeurs anormales

```python
alertes = df_stream.filter(col("valeur") > seuil_anomalie)
```

Sélectionne uniquement les mesures dont la valeur dépasse le seuil fixé (`seuil_anomalie = 35.0`).

### 11.6. Modes de sortie

- **`complete`** (statistiques) : affiche toute la table des résultats agrégés à chaque
  micro-batch.
- **`append`** (alertes) : affiche uniquement les nouvelles lignes détectées.

## 12. Exécution de l'application

```bash
cd tp-pyspark-streaming-capteurs

# Spark Standalone
spark-submit --master spark://spark-master:7077 app.py

# Mode local
spark-submit --master local[*] app.py

# Master Spark local (nom d'hôte différent)
spark-submit --master spark://localhost:7077 app.py
```

## 13. Dépôt progressif des fichiers dans HDFS

```bash
hdfs dfs -put data/capteurs_1.csv /streaming/capteurs/
# observer les résultats dans la console Spark
hdfs dfs -put data/capteurs_2.csv /streaming/capteurs/
hdfs dfs -put data/capteurs_3.csv /streaming/capteurs/

hdfs dfs -ls /streaming/capteurs
```

## 14. Exemple de résultat attendu

Statistiques (mode `complete`) :

```
+--------------+--------------+----------+----------+--------------+
|capteur       |moyenne_valeur|valeur_min|valeur_max|nombre_mesures|
+--------------+--------------+----------+----------+--------------+
|CAPTEUR_TEMP_1|26.74         |21.9      |40.7      |5             |
|CAPTEUR_TEMP_2|30.85         |24.1      |45.3      |4             |
|CAPTEUR_HUM_1 |65.33         |60.3      |70.6      |3             |
+--------------+--------------+----------+----------+--------------+
```

Alertes (mode `append`) :

```
+---+-------------------+--------------+------+-----+
|id |timestamp          |capteur       |valeur|unite|
+---+-------------------+--------------+------+-----+
|8  |2026-03-30 09:01:15|CAPTEUR_TEMP_1|40.7  |C    |
|12 |2026-03-30 09:02:15|CAPTEUR_TEMP_2|45.3  |C    |
+---+-------------------+--------------+------+-----+
```

## 15. Variante : enregistrer les alertes dans HDFS (Parquet)

```python
output_alertes = "hdfs://namenode:8020/streaming/output/alertes"

query_alertes = alertes.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_alertes) \
    .option("checkpointLocation", checkpoint_alertes) \
    .trigger(processingTime="10 seconds") \
    .start()
```

```bash
hdfs dfs -mkdir -p /streaming/output/alertes
hdfs dfs -ls /streaming/output/alertes
```

## 16. Questions de compréhension

1. Pourquoi doit-on définir un schéma explicite en Structured Streaming ?
2. Quel est le rôle du checkpoint ?
3. Quelle est la différence entre `append` et `complete` dans `writeStream` ?
4. Pourquoi utilise-t-on `maxFilesPerTrigger` ?
5. Que se passe-t-il si on redémarre l'application avec le même checkpoint ?
6. Pourquoi Spark Structured Streaming est-il considéré comme un traitement en micro-batch ?
7. Dans quel cas Kafka serait plus adapté que HDFS comme source de streaming ?
8. Comment modifier le programme pour enregistrer les statistiques dans HDFS au lieu de les
   afficher dans la console ?
9. Quelle est la différence entre un traitement batch classique et un traitement Structured
   Streaming ?
10. Pourquoi le mode `complete` est-il utilisé pour les agrégations ?

## 17. Travail complémentaire

Voir [`../tp-pyspark-streaming-capteurs/app_avance.py`](../tp-pyspark-streaming-capteurs/app_avance.py),
qui implémente :

- un seuil d'anomalie différent selon le type de capteur (température > 35, humidité > 80) ;
- une colonne `statut` (`NORMAL` / `ANORMAL`) ;
- l'enregistrement des statistiques dans HDFS au format Parquet ;
- l'affichage uniquement des capteurs dont la moyenne dépasse un seuil donné.

## 18. Conclusion

Ce TP a permis de comprendre le fonctionnement de Spark Structured Streaming, la lecture de
fichiers en mode streaming, l'utilisation d'un schéma explicite, le calcul d'indicateurs en
continu, la détection de valeurs anormales, l'importance des checkpoints, et l'exécution d'une
application PySpark avec `spark-submit`.
