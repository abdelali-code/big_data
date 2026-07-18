import csv
import json
import os

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/etudiants_raw.csv"
CLEAN_FILE = f"{DATA_DIR}/etudiants_clean.csv"
GROUPES_FILE = f"{DATA_DIR}/etudiants_groupes.json"
STATS_FILE = f"{DATA_DIR}/statistiques_inscriptions.json"
REPORT_FILE = f"{DATA_DIR}/rapport_inscriptions.txt"


def reception_fichier():
    """
    Simule la reception du fichier des etudiants envoye par l'administration.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    etudiants = [
        ["id_etudiant", "nom", "filiere", "moyenne"],
        [1, "Amine", "II-BDDC", 15.2],
        [2, "Salma", "II-BDDC", 12.8],
        [3, "Yassine", "GI", 9.4],
        [4, "Imane", "II-BDDC", 17.6],
        [5, "Karim", "GI", 11.1],
        [6, "Nadia", "GI", 14.0],
    ]

    with open(RAW_FILE, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(etudiants)

    print("Reception du fichier des etudiants")
    print(f"Fichier recu : {RAW_FILE}")


def stockage_zone_brute():
    """
    Simule le stockage du fichier recu dans une zone brute du Data Lake.
    """
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier des etudiants n'existe pas.")

    taille = os.path.getsize(RAW_FILE)
    print("Stockage du fichier dans la zone brute")
    print(f"Taille du fichier : {taille} octets")


def validation_fichier():
    """
    Verifie que le fichier existe et que les colonnes attendues sont presentes.
    """
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier des etudiants est introuvable.")

    with open(RAW_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        header = next(reader)

    colonnes_attendues = ["id_etudiant", "nom", "filiere", "moyenne"]
    if header != colonnes_attendues:
        raise ValueError("Le schema du fichier des etudiants est incorrect.")

    print("Validation du fichier des etudiants")
    print(f"Colonnes detectees : {header}")


def nettoyage_donnees():
    """
    Nettoie les donnees et normalise le champ moyenne en float.
    """
    lignes_nettoyees = []

    with open(RAW_FILE, mode="r", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            lignes_nettoyees.append({
                "id_etudiant": row["id_etudiant"],
                "nom": row["nom"].strip(),
                "filiere": row["filiere"].strip(),
                "moyenne": float(row["moyenne"]),
            })

    with open(CLEAN_FILE, mode="w", newline="", encoding="utf-8") as output_file:
        fieldnames = ["id_etudiant", "nom", "filiere", "moyenne"]
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(lignes_nettoyees)

    print("Nettoyage des donnees")
    print(f"Fichier nettoye genere : {CLEAN_FILE}")


def affectation_groupes():
    """
    Affecte chaque etudiant a un groupe selon sa moyenne
    (regle simple : >= 12 -> Groupe A, sinon Groupe B).
    S'execute en parallele de generation_statistiques.
    """
    groupes = {}

    with open(CLEAN_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            groupe = "Groupe A" if float(row["moyenne"]) >= 12 else "Groupe B"
            groupes.setdefault(groupe, []).append(row["nom"])

    with open(GROUPES_FILE, mode="w", encoding="utf-8") as file:
        json.dump(groupes, file, indent=4, ensure_ascii=False)

    print("Affectation des etudiants aux groupes")
    print(groupes)


def generation_statistiques():
    """
    Calcule des statistiques par filiere (nombre d'etudiants, moyenne generale).
    S'execute en parallele de affectation_groupes.
    """
    stats = {}

    with open(CLEAN_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            filiere = row["filiere"]
            moyenne = float(row["moyenne"])
            entry = stats.setdefault(filiere, {"nombre_etudiants": 0, "somme_moyennes": 0.0})
            entry["nombre_etudiants"] += 1
            entry["somme_moyennes"] += moyenne

    for filiere, entry in stats.items():
        entry["moyenne_generale"] = round(entry["somme_moyennes"] / entry["nombre_etudiants"], 2)
        del entry["somme_moyennes"]

    with open(STATS_FILE, mode="w", encoding="utf-8") as file:
        json.dump(stats, file, indent=4, ensure_ascii=False)

    print("Generation des statistiques")
    print(stats)


def rapport_final():
    """
    Attend la fin des deux taches paralleles (affectation_groupes et
    generation_statistiques) puis assemble le rapport final.
    """
    with open(GROUPES_FILE, mode="r", encoding="utf-8") as file:
        groupes = json.load(file)

    with open(STATS_FILE, mode="r", encoding="utf-8") as file:
        stats = json.load(file)

    with open(REPORT_FILE, mode="w", encoding="utf-8") as report:
        report.write("Rapport final - Inscriptions des etudiants\n")
        report.write("============================================\n\n")

        report.write("Groupes\n")
        for groupe, noms in groupes.items():
            report.write(f"{groupe} : {', '.join(noms)}\n")

        report.write("\nStatistiques par filiere\n")
        for filiere, entry in stats.items():
            report.write(
                f"{filiere} : {entry['nombre_etudiants']} etudiant(s), "
                f"moyenne generale = {entry['moyenne_generale']}\n"
            )

    print("Generation du rapport final")
    print(f"Fichier rapport : {REPORT_FILE}")


with DAG(
    dag_id="pipeline_inscription_etudiants",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mini-projet", "python-operator"],
) as dag:

    reception = PythonOperator(
        task_id="reception_fichier",
        python_callable=reception_fichier,
    )

    stockage = PythonOperator(
        task_id="stockage_zone_brute",
        python_callable=stockage_zone_brute,
    )

    validation = PythonOperator(
        task_id="validation_fichier",
        python_callable=validation_fichier,
    )

    nettoyage = PythonOperator(
        task_id="nettoyage_donnees",
        python_callable=nettoyage_donnees,
    )

    affectation = PythonOperator(
        task_id="affectation_groupes",
        python_callable=affectation_groupes,
    )

    statistiques = PythonOperator(
        task_id="generation_statistiques",
        python_callable=generation_statistiques,
    )

    rapport = PythonOperator(
        task_id="rapport_final",
        python_callable=rapport_final,
    )

    reception >> stockage >> validation >> nettoyage
    nettoyage >> [affectation, statistiques]
    [affectation, statistiques] >> rapport
