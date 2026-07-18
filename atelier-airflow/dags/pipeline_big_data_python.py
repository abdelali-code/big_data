import csv
import json
import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/ventes_raw.csv"
CLEAN_FILE = f"{DATA_DIR}/ventes_clean.csv"
RESULT_FILE = f"{DATA_DIR}/resultats_ventes.json"
REPORT_FILE = f"{DATA_DIR}/rapport_pipeline.txt"


def ingestion_donnees():
    """
    Cette tache simule l'ingestion de donnees depuis une source externe.
    Dans un vrai projet, la source peut etre une API, une base de donnees,
    Kafka, un fichier CSV ou un systeme transactionnel.
    """

    os.makedirs(DATA_DIR, exist_ok=True)

    ventes = [
        ["id_vente", "ville", "produit", "prix", "quantite"],
        [1, "Casablanca", "PC", 8000, 2],
        [2, "Rabat", "Clavier", 300, 5],
        [3, "Marrakech", "Souris", 150, 10],
        [4, "Casablanca", "Ecran", 2500, 3],
        [5, "Tanger", "PC", 8500, 1],
        [6, "Rabat", "Ecran", 2300, 2],
    ]

    with open(RAW_FILE, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(ventes)

    print(f"Ingestion terminee. Fichier cree : {RAW_FILE}")


def stockage_zone_brute():
    """
    Cette tache simule le stockage des donnees dans une zone brute.
    Dans un vrai pipeline Big Data, cette zone peut etre HDFS, MinIO,
    Amazon S3 ou un Data Lake.
    """

    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier brut n'existe pas.")

    taille = os.path.getsize(RAW_FILE)

    print("Stockage dans la zone brute termine.")
    print(f"Fichier brut : {RAW_FILE}")
    print(f"Taille du fichier : {taille} octets")


def validation_donnees():
    """
    Cette tache verifie que les donnees existent et que les colonnes
    attendues sont presentes.
    """

    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier de donnees est introuvable.")

    with open(RAW_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        header = next(reader)

    colonnes_attendues = ["id_vente", "ville", "produit", "prix", "quantite"]

    if header != colonnes_attendues:
        raise ValueError("Le schema du fichier est incorrect.")

    print("Validation terminee avec succes.")
    print(f"Colonnes detectees : {header}")


def transformation_donnees():
    """
    Cette tache simule le nettoyage et la transformation des donnees.
    Elle calcule une nouvelle colonne : montant = prix * quantite.
    """

    lignes_nettoyees = []

    with open(RAW_FILE, mode="r", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)

        for row in reader:
            prix = float(row["prix"])
            quantite = int(row["quantite"])
            montant = prix * quantite

            lignes_nettoyees.append({
                "id_vente": row["id_vente"],
                "ville": row["ville"],
                "produit": row["produit"],
                "prix": prix,
                "quantite": quantite,
                "montant": montant,
            })

    with open(CLEAN_FILE, mode="w", newline="", encoding="utf-8") as output_file:
        fieldnames = ["id_vente", "ville", "produit", "prix", "quantite", "montant"]
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(lignes_nettoyees)

    print("Transformation terminee.")
    print(f"Fichier nettoye genere : {CLEAN_FILE}")


def traitement_analytique():
    """
    Cette tache simule un traitement analytique Big Data.
    Elle calcule le chiffre d'affaires total par ville.
    """

    ca_par_ville = {}

    with open(CLEAN_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            ville = row["ville"]
            montant = float(row["montant"])
            ca_par_ville[ville] = ca_par_ville.get(ville, 0) + montant

    with open(RESULT_FILE, mode="w", encoding="utf-8") as file:
        json.dump(ca_par_ville, file, indent=4, ensure_ascii=False)

    print("Traitement analytique termine.")
    print("Chiffre d'affaires par ville :")
    print(ca_par_ville)


def chargement_resultats():
    """
    Cette tache simule le chargement des resultats dans une base analytique
    ou un Data Warehouse.
    """

    if not os.path.exists(RESULT_FILE):
        raise FileNotFoundError("Le fichier des resultats est introuvable.")

    print("Chargement des resultats termine.")
    print(f"Resultats disponibles dans : {RESULT_FILE}")


def generation_rapport():
    """
    Cette tache genere un petit rapport final du pipeline.
    """

    with open(RESULT_FILE, mode="r", encoding="utf-8") as file:
        resultats = json.load(file)

    with open(REPORT_FILE, mode="w", encoding="utf-8") as report:
        report.write("Rapport du pipeline Big Data\n")
        report.write("============================\n\n")

        for ville, ca in resultats.items():
            report.write(f"{ville} : {ca} DH\n")

    print("Rapport final genere.")
    print(f"Fichier rapport : {REPORT_FILE}")


with DAG(
    dag_id="pipeline_big_data_python",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["big-data", "python-operator", "pipeline"],
) as dag:

    ingestion = PythonOperator(
        task_id="ingestion_donnees",
        python_callable=ingestion_donnees,
    )

    stockage = PythonOperator(
        task_id="stockage_zone_brute",
        python_callable=stockage_zone_brute,
    )

    validation = PythonOperator(
        task_id="validation_donnees",
        python_callable=validation_donnees,
    )

    transformation = PythonOperator(
        task_id="transformation_donnees",
        python_callable=transformation_donnees,
    )

    traitement = PythonOperator(
        task_id="traitement_analytique",
        python_callable=traitement_analytique,
    )

    chargement = PythonOperator(
        task_id="chargement_resultats",
        python_callable=chargement_resultats,
    )

    rapport = PythonOperator(
        task_id="generation_rapport",
        python_callable=generation_rapport,
    )

    ingestion >> stockage >> validation >> transformation >> traitement >> chargement >> rapport
