"""
TP 4 - Variante avancee ("Travail complementaire", section 17 du sujet).

Ajouts par rapport a app.py :
  - seuil d'anomalie different selon le type de capteur
    (temperature > 35, humidite > 80, sinon pas de seuil connu -> NORMAL) ;
  - colonne "statut" (NORMAL / ANORMAL) ajoutee a chaque mesure ;
  - statistiques enregistrees dans HDFS au format Parquet (au lieu de la console) ;
  - affichage uniquement des capteurs dont la moyenne depasse un seuil donne.

Usage cluster (HDFS + Spark) :
    spark-submit --master spark://spark-master:7077 app_avance.py

Usage local (venv, sans Docker) :
    python app_avance.py --local
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, when
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)

SEUIL_TEMPERATURE = 35.0
SEUIL_HUMIDITE = 80.0
SEUIL_MOYENNE_AFFICHAGE = 25.0  # n'affiche que les capteurs dont la moyenne depasse ce seuil


def parse_args():
    parser = argparse.ArgumentParser(description="TP Structured Streaming - Capteurs (variante avancee)")
    parser.add_argument("--local", action="store_true",
                         help="Utilise le systeme de fichiers local au lieu de HDFS.")
    parser.add_argument("--source", default=None)
    parser.add_argument("--checkpoint-stats", default=None)
    parser.add_argument("--checkpoint-alertes", default=None)
    parser.add_argument("--output-stats", default=None,
                         help="Dossier de sortie Parquet pour les statistiques.")
    parser.add_argument("--master", default=None)
    return parser.parse_args()


def main():
    args = parse_args()

    if args.local:
        source_path = args.source or "data"
        checkpoint_stats = args.checkpoint_stats or "checkpoints/capteurs_stats_avance"
        checkpoint_alertes = args.checkpoint_alertes or "checkpoints/capteurs_alertes_avance"
        output_stats = args.output_stats or "output/capteurs_stats_parquet"
    else:
        source_path = args.source or "hdfs://namenode:8020/streaming/capteurs"
        checkpoint_stats = args.checkpoint_stats or "hdfs://namenode:8020/streaming/checkpoints/capteurs_stats_avance"
        checkpoint_alertes = args.checkpoint_alertes or "hdfs://namenode:8020/streaming/checkpoints/capteurs_alertes_avance"
        output_stats = args.output_stats or "hdfs://namenode:8020/streaming/output/capteurs_stats_parquet"

    builder = SparkSession.builder.appName("TP PySpark Structured Streaming - Capteurs (avance)")
    if args.master:
        builder = builder.master(args.master)
    elif args.local:
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema_capteurs = StructType([
        StructField("id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("capteur", StringType(), True),
        StructField("valeur", DoubleType(), True),
        StructField("unite", StringType(), True),
    ])

    df_stream = spark.readStream \
        .option("header", "true") \
        .option("maxFilesPerTrigger", 1) \
        .schema(schema_capteurs) \
        .csv(source_path)

    df_stream.printSchema()

    # Seuil d'anomalie selon le type de capteur (deduit du nom, ex: CAPTEUR_TEMP_1, CAPTEUR_HUM_1)
    seuil_capteur = when(col("capteur").contains("TEMP"), SEUIL_TEMPERATURE) \
        .when(col("capteur").contains("HUM"), SEUIL_HUMIDITE) \
        .otherwise(None)

    df_avec_statut = df_stream.withColumn(
        "statut",
        when(col("valeur") > seuil_capteur, "ANORMAL").otherwise("NORMAL"),
    )

    # Statistiques par capteur, avec filtre sur la moyenne
    stats_capteurs = df_avec_statut.groupBy("capteur") \
        .agg(
            avg("valeur").alias("moyenne_valeur"),
            min("valeur").alias("valeur_min"),
            max("valeur").alias("valeur_max"),
            count("*").alias("nombre_mesures"),
        ) \
        .filter(col("moyenne_valeur") > SEUIL_MOYENNE_AFFICHAGE)

    # Mesures anormales, avec le statut et le seuil qui a servi a la decision
    alertes = df_avec_statut.filter(col("statut") == "ANORMAL") \
        .select("id", "timestamp", "capteur", "valeur", "unite", "statut")

    # Statistiques -> HDFS au format Parquet (mode complete non supporte en append/parquet,
    # on utilise donc "complete" avec le format "memory" n'est pas souhaite ici ;
    # pour persister une agregation "complete" en continu, on ecrit en console
    # ET en Parquet via foreachBatch afin d'overwrite le fichier a chaque micro-batch).
    def write_stats_batch(batch_df, batch_id):
        batch_df.write.mode("overwrite").parquet(output_stats)

    query_stats = stats_capteurs.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_stats_batch) \
        .option("checkpointLocation", checkpoint_stats) \
        .trigger(processingTime="10 seconds") \
        .start()

    # Alertes -> console (mode append, compatible avec un format append classique)
    query_alertes = alertes.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_alertes) \
        .trigger(processingTime="10 seconds") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
