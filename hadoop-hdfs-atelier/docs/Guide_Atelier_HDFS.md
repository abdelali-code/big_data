# Atelier HDFS — Exécution locale et préparation du livrable

Ce document décrit la procédure complète pour lancer le cluster Hadoop HDFS, exécuter les commandes de l'exercice de synthèse, et produire les cinq livrables demandés.

---

## 1. Prérequis

Vérifier l'installation de Docker :

```bash
docker --version
docker compose version
```

---

## 2. Préparer le dossier de travail

Créer un dossier de travail contenant `docker-compose.yml`, `config` et un dossier `volumes/`.

```bash
mkdir -p atelier-hadoop-hdfs/volumes
cd atelier-hadoop-hdfs
```

Arborescence attendue :

```
atelier-hadoop-hdfs/
├── docker-compose.yml
├── config
└── volumes/
```

### 2.1 Fichier `docker-compose.yml`

```yaml
services:
  namenode:
    image: apache/hadoop:3.4.3
    hostname: namenode
    command: ["hdfs", "namenode"]
    ports:
      - "9870:9870"
      - "8020:8020"
    env_file:
      - ./config
    environment:
      ENSURE_NAMENODE_DIR: "/tmp/hadoop-root/dfs/name"
    volumes:
      - ./volumes/namenode:/data
  datanode1:
    image: apache/hadoop:3.4.3
    hostname: datanode1
    command: ["hdfs", "datanode"]
    env_file:
      - ./config
  datanode2:
    image: apache/hadoop:3.4.3
    hostname: datanode2
    command: ["hdfs", "datanode"]
    env_file:
      - ./config
  datanode3:
    image: apache/hadoop:3.4.3
    hostname: datanode3
    command: ["hdfs", "datanode"]
    env_file:
      - ./config
  datanode4:
    image: apache/hadoop:3.4.3
    hostname: datanode4
    command: ["hdfs", "datanode"]
    env_file:
      - ./config
  datanode5:
    image: apache/hadoop:3.4.3
    hostname: datanode5
    command: ["hdfs", "datanode"]
    env_file:
      - ./config
```

Les services `resourcemanager` et `nodemanager` ne sont pas nécessaires pour cet atelier (aucun traitement MapReduce) et peuvent être retirés.

### 2.2 Fichier `config` (sans extension)

```
HADOOP_HOME=/opt/hadoop
CORE-SITE.XML_fs.defaultFS=hdfs://namenode:8020
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020
HDFS-SITE.XML_dfs.replication=3
HDFS-SITE.XML_dfs.blocksize=134217728
HDFS-SITE.XML_dfs.webhdfs.enabled=true
```

Le fichier `config` doit être enregistré avec des fins de ligne Unix (LF). La valeur de `fs.defaultFS` inclut le port `:8020` pour correspondre à `dfs.namenode.rpc-address`.

---

## 3. Lancer le cluster

```bash
docker compose up -d
docker compose ps
```

Les conteneurs attendus sont `namenode` et `datanode1` à `datanode5`, tous en état *Up*. Consulter les logs si besoin :

```bash
docker compose logs namenode
docker compose logs datanode1
```

---

## 4. Accéder à l'interface web du NameNode

Ouvrir dans un navigateur : **http://localhost:9870**

Cette interface donne accès à l'état général du cluster (*Overview*), à la liste des DataNodes actifs (*Datanodes*) et au navigateur de fichiers HDFS (*Utilities → Browse the file system*). La capture d'écran du livrable est réalisée depuis cette interface, en affichant les cinq DataNodes actifs.

---

## 5. Se connecter au conteneur NameNode

Toutes les commandes HDFS s'exécutent depuis le conteneur `namenode` :

```bash
docker compose exec namenode bash
```

Vérifications initiales :

```bash
hadoop version
hdfs dfs -ls /
```

---

## 6. Exercice de synthèse — commandes

Exécuter les commandes suivantes dans le conteneur `namenode`.

```bash
# 1. Dossier principal
hdfs dfs -mkdir /exercice

# 2. Sous-dossiers
hdfs dfs -mkdir /exercice/raw
hdfs dfs -mkdir /exercice/archive
hdfs dfs -mkdir /exercice/export

# 3. Créer le fichier CSV local
cat > /tmp/clients.csv << 'EOF'
id_client,nom,ville,pays
1,Ahmed,Casablanca,Maroc
2,Fatima,Rabat,Maroc
3,Youssef,Fes,Maroc
4,Sara,Marrakech,Maroc
EOF

# 4. Envoyer le fichier dans /exercice/raw
hdfs dfs -put /tmp/clients.csv /exercice/raw/

# 5. Lire le fichier depuis HDFS
hdfs dfs -cat /exercice/raw/clients.csv

# 6. Copier vers /exercice/archive
hdfs dfs -cp /exercice/raw/clients.csv /exercice/archive/

# 7. Télécharger depuis HDFS vers le local
mkdir -p /tmp/export
hdfs dfs -get /exercice/raw/clients.csv /tmp/export/
ls -lh /tmp/export

# 8. Afficher la taille
hdfs dfs -du -h /exercice/raw

# 9. Vérifier les blocs
hdfs fsck /exercice/raw/clients.csv -files -blocks -locations

# 10. Modifier le facteur de réplication à 3
hdfs dfs -setrep -w 3 /exercice/raw/clients.csv
```

Sortir du conteneur :

```bash
exit
```

---

## 7. Préparer les livrables

### Livrable 1 — Commandes utilisées

La liste des commandes correspond à la section 6.

### Livrable 2 — Capture de l'interface web

Depuis **http://localhost:9870**, onglet *Overview* ou *Datanodes*, réaliser une capture d'écran montrant les cinq DataNodes actifs.

### Livrable 3 — Sortie de `hdfs dfs -ls -R /exercice`

Exécuter la commande depuis l'hôte en redirigeant la sortie dans un fichier :

```bash
docker compose exec namenode hdfs dfs -ls -R /exercice > ls_exercice.txt
```

Résultat attendu (les dates et tailles peuvent varier) :

```
drwxr-xr-x   - root supergroup          0 2026-06-15 10:20 /exercice/archive
-rw-r--r--   3 root supergroup        102 2026-06-15 10:20 /exercice/archive/clients.csv
drwxr-xr-x   - root supergroup          0 2026-06-15 10:19 /exercice/export
drwxr-xr-x   - root supergroup          0 2026-06-15 10:20 /exercice/raw
-rw-r--r--   3 root supergroup        102 2026-06-15 10:20 /exercice/raw/clients.csv
```

La valeur `3` placée après les permissions correspond au facteur de réplication. Le dossier `export` reste vide côté HDFS, le téléchargement se faisant vers le disque local.

### Livrable 4 — Sortie de `hdfs fsck`

```bash
docker compose exec namenode hdfs fsck /exercice/raw/clients.csv -files -blocks -locations > fsck_clients.txt
```

Résultat attendu (identifiant de bloc et adresses des DataNodes variables) :

```
Connecting to namenode via http://namenode:9870/fsck?...
FSCK started by root for path /exercice/raw/clients.csv
/exercice/raw/clients.csv 102 bytes, replicated: replication=3, 1 block(s):  OK
0. BP-xxxx:blk_1073741xxx_1xxx len=102 Live_repl=3
   [DatanodeInfoWithStorage[172.20.0.3:9866,...],
    DatanodeInfoWithStorage[172.20.0.5:9866,...],
    DatanodeInfoWithStorage[172.20.0.6:9866,...]]

Status: HEALTHY
 Total size:    102 B
 Total blocks (validated):  1
 Average block replication: 3.0
 Missing replicas:          0
 Number of data-nodes:      5
The filesystem under path '/exercice/raw/clients.csv' is HEALTHY
```

Éléments à observer : un seul bloc (fichier inférieur à 128 Mo), un facteur de réplication égal à 3, un statut HEALTHY, et un bloc présent sur trois DataNodes distincts.

### Livrable 5 — Explication du rôle de la réplication

Voir la section 8.

### Regrouper les sorties dans un fichier

```bash
{
  echo "===== ls -R /exercice ====="
  docker compose exec -T namenode hdfs dfs -ls -R /exercice
  echo; echo "===== fsck clients.csv ====="
  docker compose exec -T namenode hdfs fsck /exercice/raw/clients.csv -files -blocks -locations
} > livrable_sorties.txt
```

---

## 8. Rôle de la réplication

La réplication consiste à conserver plusieurs copies d'un même bloc de données (ici 3, valeur du paramètre `dfs.replication`) sur des DataNodes différents. Son but est d'assurer la tolérance aux pannes et la haute disponibilité : lorsqu'un DataNode devient indisponible, le NameNode continue de servir le fichier à partir d'une autre copie, puis recrée automatiquement les copies manquantes sur d'autres nœuds afin de revenir au facteur de réplication demandé. Elle améliore également la localité des lectures, un client pouvant lire la copie la plus proche. Le coût de ce mécanisme est un surcoût de stockage : un fichier occupe N fois sa taille réelle, N étant le facteur de réplication.

---

## 9. Dépannage

Le NameNode ne démarre pas :

```bash
docker compose logs namenode
docker compose down
docker compose up -d
```

En dernier recours, réinitialiser l'état du cluster (cette commande efface les données persistées localement) :

```bash
docker compose down
rm -rf volumes
docker compose up -d
```

Les DataNodes n'apparaissent pas : vérifier que le fichier `config` contient les lignes suivantes, puis attendre l'enregistrement des nœuds avant de relancer `hdfs dfsadmin -report` :

```
CORE-SITE.XML_fs.defaultFS=hdfs://namenode:8020
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020
```

L'interface web ne répond pas : vérifier la présence de `- "9870:9870"` dans `docker-compose.yml`, puis recharger `http://localhost:9870`.

Erreur de fichier déjà existant avec `-put` : supprimer l'ancien fichier ou forcer le remplacement :

```bash
hdfs dfs -put -f /tmp/clients.csv /exercice/raw/
```

---

## 10. Récapitulatif des livrables

| # | Livrable | Obtention |
|---|----------|-----------|
| 1 | Liste des commandes | Section 6 |
| 2 | Capture web NameNode | `http://localhost:9870` |
| 3 | Sortie `ls -R /exercice` | `ls_exercice.txt` |
| 4 | Sortie `fsck` | `fsck_clients.txt` |
| 5 | Explication réplication | Section 8 |
