# Atelier Apache Airflow — Orchestration de pipelines Big Data

Solution complète de l'atelier + mini-cours pour comprendre chaque brique avant de l'utiliser.

## Structure du projet

```
atelier-airflow/
├── docker-compose.yaml
├── dags/
│   ├── mon_premier_dag.py                  # Exercice 1 : logique de base
│   ├── pipeline_big_data_python.py          # Exercice 2 : pipeline linéaire complet
│   ├── pipeline_big_data_parallele.py       # Exercice 3 : branches parallèles
│   └── pipeline_inscription_etudiants.py    # Mini-projet
├── logs/       # généré par Airflow (logs d'exécution)
├── plugins/    # extensions personnalisées (vide ici)
├── config/     # config additionnelle (vide ici)
└── data/       # fichiers manipulés par les DAGs (CSV, JSON, rapports)
```

---

## Partie 1 — Comprendre Airflow avant de cliquer sur "Trigger"

Avant de lancer quoi que ce soit, voici les briques que tu vas manipuler et à quoi elles servent :

| Concept | En une phrase |
|---|---|
| **DAG** | Le pipeline lui-même : un graphe orienté sans cycle de tâches à exécuter dans un ordre précis. |
| **Task** | Une étape individuelle du DAG (ex: `validation_donnees`). |
| **Operator** | Le "modèle" qui dit *comment* une tâche s'exécute. `PythonOperator` = exécute une fonction Python. `BashOperator` = exécute une commande shell. |
| **Scheduler** | Le composant qui décide *quand* déclencher un DAG (planification, ou manuellement). |
| **Worker** | Le composant qui exécute réellement le code de la tâche. |
| **Web UI** | L'interface où tu vois l'état des DAGs, la vue Graph, les logs. |

**L'idée clé à retenir** : Airflow n'exécute pas de "gros calcul" lui-même (ça, c'est le rôle de Spark, Kafka, etc.). Il **orchestre** — il sait quelle tâche vient après quelle autre, il relance en cas d'échec, il garde un historique. Pense à un chef d'orchestre : il ne joue pas du violon, il dit à chaque musicien quand jouer.

Un DAG en Python n'est **pas exécuté séquentiellement comme un script normal** : le bloc `with DAG(...) as dag:` ne fait que *déclarer* la structure (les tâches et leurs dépendances) ; c'est le scheduler qui, plus tard, décide d'exécuter chaque tâche, potentiellement sur des workers différents, à des moments différents.

---

## Partie 2 — Lancer l'environnement

### 1. Prérequis
Docker Desktop + Docker Compose installés, ~4 Go de RAM libres.

### 2. Placer les fichiers
Le dossier `atelier-airflow/` que tu as reçu contient déjà `dags/` rempli et le `docker-compose.yaml`. Ouvre un terminal dedans.

### 3. Initialiser la base Airflow
```bash
docker compose run airflow-webserver airflow db init
```
> Avec Airflow 2.8.1, tu peux voir un message suggérant `airflow db migrate` à la place — c'est la nouvelle commande recommandée, mais `db init` fonctionne toujours pour une première installation.

### 4. Créer l'utilisateur admin
```bash
docker compose run airflow-webserver airflow users create \
  --username airflow \
  --password airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@airflow.local
```

### 5. Démarrer Airflow
```bash
docker compose up -d
docker ps
```
Tu dois voir 3 conteneurs : `postgres`, `airflow-webserver`, `airflow-scheduler`.

### 6. Ouvrir l'interface
`http://localhost:8080` — identifiants `airflow` / `airflow`.

---

## Partie 3 — Exécuter les DAGs et vérifier les résultats

Pour chacun des 4 DAGs :
1. Cherche le `dag_id` dans la liste.
2. Active-le (toggle à gauche du nom).
3. Clique sur "Trigger DAG" (▶).
4. Ouvre la vue **Graph** pour regarder l'ordre d'exécution.
5. Une fois les tâches vertes, clique sur une tâche → **Logs** pour voir les `print()`.

### `mon_premier_dag`
3 tâches en série : `debut >> traitement >> fin`. Sert juste à comprendre le mécanisme.

### `pipeline_big_data_python`
Pipeline linéaire à 7 étapes qui écrit de vrais fichiers dans `./data/` sur ta machine (grâce au volume monté) :
`ventes_raw.csv` → `ventes_clean.csv` → `resultats_ventes.json` → `rapport_pipeline.txt`.
Ouvre ces fichiers après exécution pour voir le résultat concret du pipeline.

### `pipeline_big_data_parallele`
`preparation >> validation`, puis `validation >> [traitement_par_ville, traitement_par_produit]`, puis les deux convergent vers `generation_rapport_final`. Dans la vue Graph, les deux tâches parallèles apparaissent côte à côte, sans lien entre elles — Airflow peut les lancer en même temps.

### `pipeline_inscription_etudiants` (mini-projet)
Même logique que le pipeline parallèle, appliquée à un scénario d'inscriptions :
`reception_fichier >> stockage_zone_brute >> validation_fichier >> nettoyage_donnees`, puis `nettoyage_donnees >> [affectation_groupes, generation_statistiques] >> rapport_final`.
- `affectation_groupes` classe les étudiants en Groupe A (moyenne ≥ 12) / Groupe B.
- `generation_statistiques` calcule le nombre d'étudiants et la moyenne générale par filière.
- `rapport_final` fusionne les deux résultats dans `rapport_inscriptions.txt`.

---

## Partie 4 — Manipulations à faire toi-même (sections 8 et 9 du TP)

Ces deux manipulations sont volontairement **à faire en modifiant temporairement le fichier**, pas des livrables séparés — donc je ne les ai pas figées dans le code, mais voici exactement quoi faire :

### Planification automatique (section 8)
Dans `pipeline_big_data_python.py`, remplace :
```python
schedule=None,
```
par :
```python
schedule="@daily",
```
Sauvegarde, attends quelques secondes qu'Airflow détecte le changement, puis regarde la colonne "Next Run" dans la liste des DAGs. Remets `schedule=None` ensuite si tu ne veux pas qu'il se déclenche tout seul chaque jour.

### Provoquer une erreur volontaire (section 9)
Dans `pipeline_big_data_python.py`, remplace temporairement le corps de `validation_donnees` par :
```python
def validation_donnees():
    raise Exception("Erreur volontaire : les donnees ne sont pas valides.")
```
Trigger le DAG → la tâche `validation_donnees` passe en rouge, et **toutes les tâches en aval** (`transformation_donnees`, `traitement_analytique`, `chargement_resultats`, `generation_rapport`) restent grises/skip car elles dépendent d'une tâche qui a échoué. Regarde les logs de la tâche rouge : tu verras la stack trace Python complète, avec ton message d'exception. Remets ensuite le vrai code de `validation_donnees`.

Les `retries: 2` + `retry_delay: timedelta(minutes=1)` déjà présents dans `default_args` de ce DAG signifient qu'Airflow retentera automatiquement 2 fois, avec 1 minute d'attente entre chaque tentative, avant de marquer la tâche définitivement en échec — utile pour des erreurs transitoires (réseau, service momentanément indisponible), pas pour un vrai bug comme celui qu'on vient d'injecter.

---

## Partie 5 — Réponses aux questions du TP

### Section 6.3 — `pipeline_big_data_python`
1. **Première tâche exécutée** : `ingestion_donnees`.
2. **Dernière tâche exécutée** : `generation_rapport`.
3. **Tâche qui crée le fichier CSV brut** : `ingestion_donnees` (écrit `ventes_raw.csv`).
4. **Tâche qui vérifie le schéma** : `validation_donnees` (compare l'en-tête du CSV à la liste de colonnes attendues).
5. **Tâche qui calcule le chiffre d'affaires par ville** : `traitement_analytique`.
6. **Où voir les messages `print()`** : dans l'onglet **Logs** de chaque tâche, accessible en cliquant sur la tâche dans la vue Graph (ou dans la vue Grid → clic sur une case → "Logs").

### Section 11.1 — `pipeline_big_data_parallele`
1. **Tâches avant le parallélisme** : `preparation_donnees` puis `validation_donnees`.
2. **Tâches qui s'exécutent en parallèle** : `traitement_par_ville` et `traitement_par_produit`.
3. **Tâche qui attend la fin des deux** : `generation_rapport_final`.
4. **Représentation dans la vue Graph** : les deux tâches parallèles apparaissent sur la même "colonne" verticale, toutes deux reliées par une flèche entrante depuis `validation_donnees` et une flèche sortante vers `generation_rapport_final`, sans flèche entre elles — ce qui montre visuellement qu'elles n'ont pas de dépendance l'une envers l'autre et peuvent tourner en même temps.

---

## Partie 6 — Pourquoi Airflow compte dans une architecture Data Engineering

Un projet Big Data réel combine plusieurs outils spécialisés (stockage, traitement, base analytique, visualisation). Sans orchestrateur, il faudrait soit lancer chaque étape à la main, soit écrire des scripts cron fragiles qui ne savent pas gérer les dépendances entre étapes ni les échecs partiels.

Airflow apporte cinq choses concrètes que ces scripts n'ont pas nativement :
- **automatisation** : planification déclarative (`@daily`, cron) au lieu de crontabs éparpillées ;
- **supervision** : état de chaque tâche visible en un coup d'œil dans la Web UI ;
- **traçabilité** : historique complet des exécutions et logs conservés ;
- **fiabilité** : retries automatiques, alerte en cas d'échec, reprise sans tout relancer depuis zéro ;
- **maintenabilité** : le pipeline est du code Python versionnable, pas une suite de commandes manuelles.

---

## Commandes utiles

| Commande | Rôle |
|---|---|
| `docker compose up -d` | Démarrer Airflow en arrière-plan |
| `docker compose down` | Arrêter Airflow |
| `docker compose down --volumes --remove-orphans` | Tout supprimer (y compris la base Postgres) pour repartir de zéro |
| `docker ps` | Voir les conteneurs actifs |
| `docker compose logs airflow-scheduler` | Logs du scheduler |
| `docker compose logs airflow-webserver` | Logs du webserver |
| `docker compose run airflow-webserver airflow dags list` | Lister les DAGs détectés |
| `docker compose run airflow-webserver airflow tasks list mon_premier_dag` | Lister les tâches d'un DAG |

## Livrables à produire toi-même

Le code (les 4 fichiers `.py`) est prêt dans `dags/`. Il te reste à capturer, une fois les DAGs exécutés dans l'UI :
1. la liste des DAGs dans l'interface Airflow ;
2. la vue Graph d'un DAG réussi (idéalement `pipeline_big_data_parallele`, pour bien montrer les branches) ;
3. les logs d'une tâche `PythonOperator` (par exemple `validation_donnees`) ;
4. tes réponses aux questions — celles ci-dessus te servent de base, reformule-les avec tes propres mots pour le rendu.
