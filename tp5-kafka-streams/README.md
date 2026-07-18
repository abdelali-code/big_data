# TP 5 — Traitement de flux avec Kafka Streams

Solution complète des 3 exercices, en Spring Boot + Kafka Streams.

## Structure du projet

```
tp5-kafka-streams/
├── docker-compose.yml              # Kafka (KRaft, sans Zookeeper) prêt à l'emploi
├── create-topics.sh                # Crée tous les topics des 3 exercices
├── exercice1-text-cleaning/        # App Spring Boot Kafka Streams : nettoyage/validation de texte
├── exercice2-weather-analysis/     # App Spring Boot Kafka Streams : agrégation météo
└── exercice3-click-counter/
    ├── click-producer/             # App web Spring Boot : bouton -> topic "clicks"
    ├── click-streams/              # App Kafka Streams : comptage des clics
    └── click-consumer-rest/        # API REST Spring Boot : expose les compteurs
```

Chaque module Maven est indépendant (son propre `pom.xml`, `spring-boot-starter-parent`).
Tu peux les ouvrir comme 5 projets séparés dans IntelliJ, ou tous sous un seul dossier parent.

---

## 1. Démarrer Kafka

```bash
cd tp5-kafka-streams
docker compose up -d
```

Ça lance un broker Kafka en mode KRaft (pas besoin de Zookeeper) sur `localhost:9092`.

Vérifie qu'il tourne :
```bash
docker ps
docker logs kafka --tail 20
```

## 2. Créer les topics

```bash
chmod +x create-topics.sh
./create-topics.sh
```

Ce script crée : `text-input`, `text-clean`, `text-dead-letter`, `weather-data`,
`station-averages`, `clicks`, `click-counts`.

Vérifier :
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## Exercice 1 : Nettoyage et validation de messages texte

### Lancer

```bash
cd exercice1-text-cleaning
mvn spring-boot:run
```

### Architecture

`text-input` → `mapValues(clean)` → `split()` sur `isValid()` → `text-clean` / `text-dead-letter`

- Nettoyage : `trim()` + regex `\s+` → un seul espace + `toUpperCase()`.
- Validation : vide, > 100 caractères, ou contient `HACK`/`SPAM`/`XXX` (recherche faite **après**
  la mise en majuscules, donc insensible à la casse).

### Tester

Producteur :
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic text-input --bootstrap-server localhost:9092
```
Tape ensuite, par exemple :
```
Bonjour Kafka Streams
this is spam message
message contenant hack
```

Consommateurs (dans deux autres terminaux) :
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic text-clean --from-beginning --bootstrap-server localhost:9092

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic text-dead-letter --from-beginning --bootstrap-server localhost:9092
```

### Réponses aux questions

1. **Rôle de `text-dead-letter`** : c'est une file de "lettres mortes" qui isole les messages
   rejetés (invalides) du flux principal, pour qu'on puisse les auditer, les corriger ou les
   rejouer sans polluer le topic des données propres ni bloquer le traitement normal.
2. **Pourquoi nettoyer avant de traiter** : des données mal formées faussent les comparaisons
   (espaces superflus, casse différente) et peuvent faire échouer des règles métier qui
   supposent un format normalisé. Nettoyer en amont garantit que les règles de validation
   s'appliquent de façon cohérente.
3. **Pourquoi mettre en majuscules avant de vérifier les mots interdits** : pour rendre la
   détection insensible à la casse (`Hack`, `HACK`, `hack` doivent tous être détectés) sans
   avoir à écrire plusieurs variantes de chaque mot interdit.
4. **Liste de mots interdits externalisée** : au lieu d'une `List<String>` codée en dur, on
   chargerait la liste depuis un fichier de config (`application.yml`, `@ConfigurationProperties`)
   ou une base de données, avec une actualisation périodique (scheduler) ou dynamique (table
   Kafka `KTable` diffusée en `GlobalKTable` alimentée par un topic de configuration, jointe au
   flux principal). Ça permet de mettre à jour les règles sans redéployer l'application.

---

## Exercice 2 : Analyse de données météorologiques

### Lancer

```bash
cd exercice2-weather-analysis
mvn spring-boot:run
```

### Architecture

`weather-data` (CSV `station,temperature,humidity`) →
parse en `WeatherReading` (lignes mal formées filtrées, log warning) →
filtre `temperature > 30` → conversion °C→°F →
`selectKey(station)` → `groupByKey` → `aggregate` (KTable de `WeatherStats`) →
formatage en texte → `station-averages`.

### Tester

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic weather-data --bootstrap-server localhost:9092
```
```
Station1,25.3,60
Station2,35.0,50
Station2,40.0,45
Station1,32.0,70
Station3,error,60
```
(la dernière ligne est volontairement mal formée pour tester la robustesse)

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic station-averages --from-beginning \
  --bootstrap-server localhost:9092 \
  --property print.key=true
```

Résultat attendu (les valeurs se mettent à jour au fil des messages, KTable oblige) :
```
Station2 Station2 : Temperature moyenne = 99.5 F , Humidite moyenne = 47.5 %
Station1 Station1 : Temperature moyenne = 89.6 F , Humidite moyenne = 70.0 %
```

### Réponses aux questions

1. **Pourquoi regrouper par station** : les moyennes doivent être calculées séparément pour
   chaque station ; `groupByKey`/`groupBy` répartit le flux en sous-flux logiques par clé, ce
   qui est le préalable indispensable à toute agrégation par groupe.
2. **KStream vs KTable** : un `KStream` représente un flux d'événements indépendants (chaque
   enregistrement est une nouvelle information, façon "insert only" — le log d'un compte
   bancaire). Un `KTable` représente un état courant, mis à jour à chaque nouvel enregistrement
   pour une même clé (façon "upsert" — le solde actuel du compte). Un même topic peut être lu
   des deux façons selon la sémantique voulue.
3. **Pourquoi une agrégation produit une KTable** : agréger, c'est maintenir un résultat courant
   par clé (une moyenne, un total) qui évolue à chaque nouvel événement. C'est exactement la
   sémantique d'un `KTable` : il y a un état à un instant T par clé, pas une suite d'événements
   indépendants.
4. **Message mal formé (`Station1,error,60`)** : le parsing (`Double.parseDouble`) est fait dans
   un `try/catch` ; en cas d'échec on logue un avertissement et on renvoie `null`, qu'un `filter`
   élimine ensuite du flux — sans jamais faire planter le `StreamThread`. Variante plus robuste :
   router ces lignes vers un topic `weather-dead-letter` (même principe que l'exercice 1) au lieu
   de les jeter, pour pouvoir les analyser après coup.
5. **Pourquoi Kafka Streams est adapté** : les mesures arrivent en continu, en temps réel, depuis
   plusieurs sources (stations) et sans fin définie. Kafka Streams permet de traiter ce flux
   incrémentalement (pas de traitement batch qui attend une fenêtre complète de données), de
   maintenir un état agrégé tolérant aux pannes (state store + changelog topic), et de scaler
   horizontalement en ajoutant des instances/partitions.

---

## Exercice 3 : Comptage de clics (Spring Boot + Kafka Streams)

Trois applications à lancer, dans trois terminaux séparés (ports différents pour ne pas
entrer en conflit) :

```bash
cd exercice3-click-counter/click-producer && mvn spring-boot:run       # port 8082
cd exercice3-click-counter/click-streams && mvn spring-boot:run        # pas de port web exposé
cd exercice3-click-counter/click-consumer-rest && mvn spring-boot:run  # port 8083
```

### Architecture

```
[Navigateur : bouton "Cliquez ici"]
        │  POST /api/click {"userId": "user1"}
        ▼
[click-producer : Spring Boot Web]
        │  KafkaTemplate.send("clicks", userId, "click")
        ▼
   topic "clicks"
        │
        ▼
[click-streams : Kafka Streams]
        │  groupByKey().count()  → comptage par utilisateur
        │  toStream().to("click-counts")
        ▼
   topic "click-counts"  (clé = userId, valeur = compteur)
        │
        ▼
[click-consumer-rest : @KafkaListener alimente une Map en mémoire]
        │
        ▼
   GET /clicks/count        → {"user1": 45, "user2": 30}
   GET /clicks/count/total  → {"totalClicks": 125}
```

C'est la **variante 2** (comptage par utilisateur) qui est implémentée par défaut ; le
commentaire dans `ClickStreamsTopologyConfig` explique comment basculer en **variante 1**
(comptage global) en une ligne.

### Tester

1. Ouvre `http://localhost:8082` dans le navigateur, entre un `userId` (ou laisse `user1`),
   clique plusieurs fois sur le bouton.
2. Interroge l'API REST :
```bash
curl http://localhost:8083/clicks/count
curl http://localhost:8083/clicks/count/total
```
3. Tu peux aussi observer le topic directement :
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic click-counts --from-beginning \
  --bootstrap-server localhost:9092 \
  --property print.key=true
```

---

## Difficultés courantes et solutions

- **`UnknownTopicOrPartitionException` au démarrage** : le topic n'existe pas encore — exécute
  `create-topics.sh` avant de lancer les apps Streams, ou active
  `spring.kafka.streams.auto-startup` après création des topics.
- **L'application Streams ne redémarre pas proprement après un crash** : Kafka Streams garde un
  `state.dir` local (`/tmp/kafka-streams/<application-id>` par défaut) ; en cas d'état
  incohérent en dev, supprime ce dossier avant de relancer.
- **Deux applications Streams avec le même `application-id`** : elles rejoignent le même groupe
  de consommateurs et se partagent les partitions — utile pour scaler, mais assure-toi que
  `application-id` est unique par exercice, ce qui est déjà le cas ici
  (`text-cleaning-app`, `weather-analysis-app`, `click-streams-app`).
- **Arrêt propre** : avec `@EnableKafkaStreams` de Spring Kafka, le `StreamsBuilderFactoryBean`
  ferme automatiquement le `KafkaStreams` (`close()`) quand le contexte Spring s'arrête — le hook
  d'arrêt demandé par l'énoncé (exercice 2) est donc déjà géré, pas besoin d'ajouter
  `Runtime.getRuntime().addShutdownHook(...)` manuellement.
