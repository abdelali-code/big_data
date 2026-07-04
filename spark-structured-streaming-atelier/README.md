# Spark Structured Streaming Atelier (capteurs + HDFS + Docker Compose)

TP 4 : traitement en temps quasi reel de mesures de capteurs avec **PySpark Structured
Streaming**, lues au fil de l'eau depuis un dossier **HDFS**, sur un cluster **HDFS + YARN +
Spark Standalone** lance avec Docker Compose.

## Repository structure

```
.
├── docker-compose.yml   # NameNode + 5 DataNodes + ResourceManager/NodeManager (YARN)
│                        # + Spark Standalone (1 master + 2 workers)
├── config               # Hadoop/YARN configuration (env_file)
├── .gitignore            # ignores volumes/, .venv/, checkpoints/, output/
├── README.md             # this file
├── docs/
│   └── Guide_TP4_Structured_Streaming.md   # full step-by-step guide (source PDF, FR)
└── tp-pyspark-streaming-capteurs/
    ├── requirements.txt   # pyspark==4.1.1
    ├── app.py             # main TP app (matches the subject's code exactly)
    ├── app_avance.py       # bonus "Travail complementaire" (section 17)
    └── data/
        ├── capteurs_1.csv
        ├── capteurs_2.csv
        └── capteurs_3.csv
```

> `volumes/`, `.venv/`, `checkpoints/` and `output/` are git-ignored — they are local/runtime
> state, not source.

## Prerequisites

- Docker + Docker Compose (to run the HDFS/YARN/Spark cluster)
- Python 3.9+ (to create a virtual environment and test the app)

```bash
docker --version
docker compose version
python3 --version
```

## 1. Python virtual environment (`.venv`)

The TP app is plain PySpark, so a project-local virtualenv is enough to write/test the code —
you don't need Spark installed on the host, `pyspark` ships its own Spark runtime (you do need
a JDK, e.g. `java -version` should work — JDK 17 was used to validate this atelier).

```bash
cd spark-structured-streaming-atelier/tp-pyspark-streaming-capteurs

# Create the venv (once)
python3 -m venv .venv

# Activate it
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Install dependencies (do NOT run `pip install --upgrade pip` first — see note below)
pip install -r requirements.txt

# Deactivate when done
deactivate
```

> **Note:** don't upgrade pip in this venv before installing `pyspark`. Recent pip versions
> (23.1+) force a PEP 517 wheel build for `pyspark`, which fails to build for this release.
> The default pip shipped with a fresh `python3 -m venv` (or any pip ≤ 23.0.x) falls back to
> the legacy `setup.py install` path instead, which installs cleanly. This has been verified
> end-to-end for this atelier — `pip --version` right after `python3 -m venv .venv` should
> print something ≤ 23.x.

### Quick local smoke test (no Docker/HDFS needed)

`app.py` accepts `--local` to read/write on the local filesystem instead of HDFS (relative to
`tp-pyspark-streaming-capteurs/`), so you can validate the transformation logic before
touching the cluster:

```bash
source .venv/bin/activate
python app.py --local
```

Spark will pick up `data/capteurs_1.csv`, `capteurs_2.csv`, `capteurs_3.csv` (one per
micro-batch, since `maxFilesPerTrigger=1`, one batch every 10s) and print the running stats +
alerts to the console. Stop it with `Ctrl+C` once you've seen the 3 batches — the final stats
batch should show:

```
+--------------+-----------------+----------+----------+--------------+
|capteur       |moyenne_valeur   |valeur_min|valeur_max|nombre_mesures|
+--------------+-----------------+----------+----------+--------------+
|CAPTEUR_TEMP_2|30.85            |24.1      |45.3      |4             |
|CAPTEUR_HUM_1 |65.33333333333333|60.3      |70.6      |3             |
|CAPTEUR_TEMP_1|26.74            |21.9      |40.7      |5             |
+--------------+-----------------+----------+----------+--------------+
```

with alerts fired for `id=8` (`CAPTEUR_TEMP_1`, `40.7`) and `id=12` (`CAPTEUR_TEMP_2`, `45.3`)
— this exact run has been verified while preparing this atelier.

The bonus variant works the same way and additionally writes Parquet stats locally:

```bash
python app_avance.py --local
```

To rerun from scratch locally, delete the local checkpoints/output:

```bash
rm -rf checkpoints/ output/
```

## 2. Start the HDFS + YARN + Spark cluster

```bash
cd spark-structured-streaming-atelier

docker compose up -d
docker compose ps      # namenode, datanode1..5, resourcemanager, nodemanager,
                        # spark-master, spark-worker-1, spark-worker-2 should be "Up"
```

- NameNode UI: http://localhost:9870
- ResourceManager UI: http://localhost:8088
- Spark Master UI: http://localhost:8080

## 3. Prepare HDFS folders

```bash
docker compose exec namenode bash
```

Inside the container:

```bash
hdfs dfs -mkdir -p /streaming/capteurs
hdfs dfs -mkdir -p /streaming/checkpoints/capteurs_stats
hdfs dfs -mkdir -p /streaming/checkpoints/capteurs_alertes

hdfs dfs -ls /streaming
hdfs dfs -ls /streaming/checkpoints
```

To restart the TP from scratch:

```bash
hdfs dfs -rm -r /streaming/capteurs/*
hdfs dfs -rm -r /streaming/checkpoints/capteurs_stats/*
hdfs dfs -rm -r /streaming/checkpoints/capteurs_alertes/*
```

## 4. Run the streaming application

From the host, copy the project into a container that has `spark-submit` (the `spark-master`
image), or mount the project folder. Simplest with this compose file: copy the script in and
submit it against the Spark Standalone master, pointing it at HDFS:

```bash
docker cp tp-pyspark-streaming-capteurs/app.py spark-master:/opt/spark/app.py
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/app.py
```

Local mode also works if you just want to see it run against HDFS without the Standalone
scheduler:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master local[*] /opt/spark/app.py
```

## 5. Feed files into HDFS progressively

In another terminal:

```bash
docker compose exec namenode bash
hdfs dfs -put /path/to/capteurs_1.csv /streaming/capteurs/
# watch the Spark console output, then:
hdfs dfs -put /path/to/capteurs_2.csv /streaming/capteurs/
hdfs dfs -put /path/to/capteurs_3.csv /streaming/capteurs/

hdfs dfs -ls /streaming/capteurs
```

(You'll first need to get `capteurs_1.csv`/`_2`/`_3` into the namenode container, e.g.
`docker cp tp-pyspark-streaming-capteurs/data/capteurs_1.csv namenode:/tmp/`.)

## Expected output

After each file lands in `/streaming/capteurs`, the stats query (mode `complete`) reprints the
whole aggregated table, e.g.:

```
+--------------+--------------+----------+----------+--------------+
|capteur       |moyenne_valeur|valeur_min|valeur_max|nombre_mesures|
+--------------+--------------+----------+----------+--------------+
|CAPTEUR_TEMP_1|26.74         |21.9      |40.7      |5             |
|CAPTEUR_TEMP_2|30.85         |24.1      |45.3      |4             |
|CAPTEUR_HUM_1 |65.33         |60.3      |70.6      |3             |
+--------------+--------------+----------+----------+--------------+
```

and the alerts query (mode `append`) prints only newly-detected anomalies (`valeur > 35.0`):

```
+---+-------------------+--------------+------+-----+
|id |timestamp          |capteur       |valeur|unite|
+---+-------------------+--------------+------+-----+
|8  |2026-03-30 09:01:15|CAPTEUR_TEMP_1|40.7  |C    |
|12 |2026-03-30 09:02:15|CAPTEUR_TEMP_2|45.3  |C    |
+---+-------------------+--------------+------+-----+
```

## `app.py` vs `app_avance.py`

- **`app.py`** reproduces the subject's code exactly: fixed threshold (35.0), console output
  for both stats and alerts.
- **`app_avance.py`** implements the "Travail complementaire" (section 17):
  - a threshold **per sensor type** (temperature > 35, humidity > 80, deduced from the sensor
    name `CAPTEUR_TEMP_*` / `CAPTEUR_HUM_*`);
  - a `statut` column (`NORMAL` / `ANORMAL`) added to every reading;
  - stats persisted to HDFS in **Parquet** (via `foreachBatch`, since a `complete`-mode
    aggregation can't be sunk directly to a file format that doesn't support it);
  - only sensors whose **average** exceeds `SEUIL_MOYENNE_AFFICHAGE` are shown.

Both scripts accept `--local` for filesystem-only testing and `--source` /
`--checkpoint-stats` / `--checkpoint-alertes` / `--output-stats` to override paths.

## Troubleshooting

- **DataNodes don't appear:** confirm `config` contains `fs.defaultFS=hdfs://namenode:8020`,
  wait ~30s, then `hdfs dfsadmin -report`. Ensure `config` uses **LF** line endings.
- **`pip install -r requirements.txt` fails with "Failed building wheel for pyspark":** you
  likely upgraded pip in the venv first. See the note in section 1 — recreate the venv and
  install `pyspark` before (or instead of) upgrading pip.
- **First `pip install` is slow / downloads ~455 MB:** `pyspark` bundles the full Spark
  distribution. It's a one-time download cached by pip (`~/.cache/pip`).
- **Nothing happens after `-put`:** Structured Streaming polls the directory; give it up to
  one trigger interval (10s here) to notice the new file.
- **Restarting with the same checkpoint reprocesses nothing new:** that's expected — the
  checkpoint remembers which files were already read. Wipe it (`hdfs dfs -rm -r
  .../checkpoints/...`) to reprocess from scratch.

## Full guide

The original TP subject (in French), including the full annotated code, expected outputs and
comprehension questions, is available in
[`docs/Guide_TP4_Structured_Streaming.md`](docs/Guide_TP4_Structured_Streaming.md).
