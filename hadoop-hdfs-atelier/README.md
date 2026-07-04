# Hadoop HDFS Atelier (Docker Compose)

A ready-to-run **Hadoop HDFS** cluster for learning distributed storage: files, directories,
blocks, replication and fault tolerance. Everything runs locally with Docker Compose — no
manual Hadoop installation required.

This repository focuses on **HDFS only** (no MapReduce/Spark/Hive).

## Cluster architecture

- **1 NameNode** — stores metadata (file names, paths, block-to-DataNode mapping, replication info). Web UI on port `9870`, RPC on `8020`.
- **5 DataNodes** — store the actual data blocks.
- **Replication factor: 3** — each block is kept on 3 different DataNodes.
- **Block size: 128 MB** (`dfs.blocksize=134217728`).

## Repository structure

```
.
├── docker-compose.yml   # NameNode + 5 DataNodes
├── config               # Hadoop configuration (env_file)
├── .gitignore           # ignores volumes/ and local outputs
├── README.md            # this file
└── docs/
    └── Guide_Atelier_HDFS.md   # full step-by-step guide (FR)
```

> `volumes/` (cluster state) is intentionally **git-ignored**. It is created automatically at first run.

## Prerequisites

- Docker
- Docker Compose

```bash
docker --version
docker compose version
```

## Quick start

```bash

# 2. Start the cluster
docker compose up -d
docker compose ps          # namenode + datanode1..5 should be "Up"

# 3. Open the NameNode web UI
#    http://localhost:9870   (Overview / Datanodes / Browse the file system)

# 4. Enter the NameNode container to run HDFS commands
docker compose exec namenode bash
```

Verify the cluster from inside the container:

```bash
hdfs dfsadmin -report      # capacity, live/dead DataNodes
hdfs dfs -ls /
```

## Core HDFS commands used in the atelier

| Command | Purpose |
|---|---|
| `hdfs dfs -mkdir[-p] /path` | Create a directory (`-p` = parents) |
| `hdfs dfs -put[-f] file /path` | Upload a local file (`-f` = overwrite) |
| `hdfs dfs -get /path/file .` | Download a file to local |
| `hdfs dfs -cat / -head /path` | Read a file / its beginning |
| `hdfs dfs -cp / -mv / -rm[-r]` | Copy / move / delete |
| `hdfs dfs -du -h / -count /path` | Sizes / counts |
| `hdfs dfs -setrep -w N file` | Change replication factor |
| `hdfs dfsadmin -report` | Cluster health |
| `hdfs fsck /path -files -blocks -locations` | Blocks and their DataNode locations |

## Synthesis exercise (reproduce this)

Run inside the `namenode` container:

```bash
hdfs dfs -mkdir /exercice
hdfs dfs -mkdir /exercice/raw /exercice/archive /exercice/export

cat > /tmp/clients.csv << 'EOF'
id_client,nom,ville,pays
1,Ahmed,Casablanca,Maroc
2,Fatima,Rabat,Maroc
3,Youssef,Fes,Maroc
4,Sara,Marrakech,Maroc
EOF

hdfs dfs -put /tmp/clients.csv /exercice/raw/
hdfs dfs -cat /exercice/raw/clients.csv
hdfs dfs -cp  /exercice/raw/clients.csv /exercice/archive/
hdfs dfs -get /exercice/raw/clients.csv /tmp/export/
hdfs dfs -du -h /exercice/raw
hdfs fsck /exercice/raw/clients.csv -files -blocks -locations
hdfs dfs -setrep -w 3 /exercice/raw/clients.csv
```

Collect the required outputs from the host (into files you can submit):

```bash
docker compose exec namenode hdfs dfs -ls -R /exercice > ls_exercice.txt
docker compose exec namenode hdfs fsck /exercice/raw/clients.csv -files -blocks -locations > fsck_clients.txt
```

## Simulating a DataNode failure

```bash
docker compose stop datanode5
docker compose exec namenode hdfs dfsadmin -report
docker compose exec namenode hdfs dfs -cat /exercice/raw/clients.csv   # still readable
docker compose start datanode5
```

Because each block is replicated on 3 DataNodes, stopping one node does not cause data loss.

## Stopping / resetting

```bash
docker compose down            # stop the cluster
docker compose down && rm -rf volumes && docker compose up -d   # full reset (wipes HDFS data)
```

## Troubleshooting

- **DataNodes don't appear:** confirm `config` contains `fs.defaultFS=hdfs://namenode:8020` and `dfs.namenode.rpc-address=namenode:8020`, then wait ~30s and re-run `hdfs dfsadmin -report`. Ensure `config` uses **LF** line endings.
- **Web UI unreachable:** confirm `- "9870:9870"` is mapped in `docker-compose.yml`, then reload `http://localhost:9870`.
- **"File already exists" on `-put`:** use `hdfs dfs -put -f ...` to overwrite.

## Full guide

A complete step-by-step walkthrough (in French), including the expected command outputs and the
deliverables list, is available in [`docs/Guide_Atelier_HDFS.md`](docs/Guide_Atelier_HDFS.md).

