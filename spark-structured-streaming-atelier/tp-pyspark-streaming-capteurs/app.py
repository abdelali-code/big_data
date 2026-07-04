"""
TP 4 - Analyse en temps quasi reel des mesures de capteurs
avec PySpark Structured Streaming et HDFS.

Lit en continu les fichiers CSV deposes dans un dossier (HDFS par defaut),
calcule des statistiques par capteur (moyenne/min/max/nombre) et detecte
les mesures dont la valeur depasse un seuil fixe.

Usage (cluster HDFS + Spark, comme dans le sujet) :
    spark-submit --master spark://spark-master:7077 app.py

Usage local (sans Docker/HDFS, pour tester la logique avec le venv) :
    python app.py --local

Voir README.md pour la mise en place complete (Docker, HDFS, venv).
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)

SEUIL_ANOMALIE = 35.0


def parse_args():
    parser = argparse.ArgumentParser(description="TP Structured Streaming - Capteurs")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Lit/ecrit sur le systeme de fichiers local (./data, ./checkpoints) "
             "au lieu de HDFS. Pratique pour tester sans Docker.",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Chemin source (par defaut: hdfs://namenode:8020/streaming/capteurs, "
             "ou ./data en mode --local).",
    )
    parser.add_argument(
        "--checkpoint-stats",
        default=None,
        help="Chemin checkpoint pour les statistiques.",
    )
    parser.add_argument(
        "--checkpoint-alertes",
        default=None,
        help="Chemin checkpoint pour les alertes.",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Master Spark (ex: spark://spark-master:7077 ou local[*]).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.local:
        source_path = args.source or "data"
        checkpoint_stats = args.checkpoint_stats or "checkpoints/capteurs_stats"
        checkpoint_alertes = args.checkpoint_alertes or "checkpoints/capteurs_alertes"
    else:
        source_path = args.source or "hdfs://namenode:8020/streaming/capteurs"
        checkpoint_stats = args.checkpoint_stats or "hdfs://namenode:8020/streaming/checkpoints/capteurs_stats"
        checkpoint_alertes = args.checkpoint_alertes or "hdfs://namenode:8020/streaming/checkpoints/capteurs_alertes"

    # Creation de la session Spark
    builder = SparkSession.builder.appName("TP PySpark Structured Streaming - Capteurs HDFS")
    if args.master:
        builder = builder.master(args.master)
    elif args.local:
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Definition du schema explicite des fichiers CSV
    schema_capteurs = StructType([
        StructField("id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("capteur", StringType(), True),
        StructField("valeur", DoubleType(), True),
        StructField("unite", StringType(), True),
    ])

    # Lecture du flux depuis la source (HDFS ou local)
    df_stream = spark.readStream \
        .option("header", "true") \
        .option("maxFilesPerTrigger", 1) \
        .schema(schema_capteurs) \
        .csv(source_path)

    # Affichage du schema
    df_stream.printSchema()

    # Calcul des statistiques par capteur
    stats_capteurs = df_stream.groupBy("capteur") \
        .agg(
            avg("valeur").alias("moyenne_valeur"),
            min("valeur").alias("valeur_min"),
            max("valeur").alias("valeur_max"),
            count("*").alias("nombre_mesures"),
        )

    # Detection des valeurs anormales
    alertes = df_stream.filter(col("valeur") > SEUIL_ANOMALIE) \
        .select("id", "timestamp", "capteur", "valeur", "unite")

    # Ecriture des statistiques dans la console
    query_stats = stats_capteurs.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_stats) \
        .trigger(processingTime="10 seconds") \
        .start()

    # Ecriture des alertes dans la console
    query_alertes = alertes.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_alertes) \
        .trigger(processingTime="10 seconds") \
        .start()

    # Attendre l'arret des traitements
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
