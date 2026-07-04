# Spark SQL Atelier (bike-sharing dataset)

A ready-to-run **Spark SQL** exercise on public bike-sharing rental data. It reproduces every
question from the TP (Exercise 1): data loading, temporary views, basic SQL queries,
aggregations, time-based analysis and user-behavior analysis. Everything runs locally with
Maven — no cluster required.

## Repository structure

```
.
├── pom.xml                                  # Spark 4.1.1 (Scala 2.13) dependencies
├── data/
│   └── bike_sharing.csv                     # 5000 bike rental transactions
├── src/main/java/com/atelier/sparksql/
│   └── SparkSQLApp.java                     # solves all TP questions
├── .gitignore                               # ignores target/ and Spark runtime junk
└── README.md                                # this file
```

## Dataset (`data/bike_sharing.csv`)

```
rental_id,user_id,age,gender,start_time,end_time,start_station,end_station,duration_minutes,price
```

| Column | Description |
|---|---|
| `rental_id` | unique ID for each rental |
| `user_id` | unique ID of the user |
| `age` | user's age |
| `gender` | M, F or Autre |
| `start_time` / `end_time` | rental start/end timestamp |
| `start_station` / `end_station` | pickup / return station |
| `duration_minutes` | length of the trip |
| `price` | rental cost in dollars |

## Prerequisites

- JDK 17
- Maven 3.8+

```bash
java -version
mvn -version
```

## Quick start

```bash
cd spark-sql-atelier

# Compile
mvn compile

# Run (reads data/bike_sharing.csv by default)
mvn compile exec:java

# Or point it at another CSV file
mvn compile exec:java -Dexec.args="/path/to/other_bike_sharing.csv"
```

You can also build a fat jar and run it directly with `java` or `spark-submit`:

```bash
mvn package
java -jar target/spark-sql-atelier.jar data/bike_sharing.csv
# or, if you have a real Spark cluster:
spark-submit --class com.atelier.sparksql.SparkSQLApp target/spark-sql-atelier.jar data/bike_sharing.csv
```

## What `SparkSQLApp.java` does

1. **Data Loading & Exploration** — loads the CSV with an explicit schema, prints it,
   shows the first 5 rows, counts total rentals.
2. **Temporary View** — registers `bike_rentals_view` via `createOrReplaceTempView`.
3. **Basic SQL Queries** — rentals longer than 30 minutes, rentals from a given start
   station, total revenue (`SUM(price)`).
4. **Aggregation Queries** — rentals per start station, average duration per station,
   the busiest station.
5. **Time-Based Analysis** — extracts `HOUR(start_time)`, counts rentals per hour to
   find peak hours, finds the most popular morning (7–12) start station.
6. **User Behavior Analysis** — average age, rentals/users by gender, rentals by age
   bucket (18–30, 31–40, 41–50, 51+).

Every query is written both as Spark SQL (`spark.sql("...")` against the temp view) so it
maps directly to the TP statements.

> Note: the TP's generic example ("Station A") doesn't exist in this real dataset — station
> names are things like `Station Centre-Ville`, `Station Gare`, `Station Aéroport Bus`, etc.
> Question 3.2 uses the constant `SAMPLE_STATION` (defaults to `Station Centre-Ville`) — change
> it in `SparkSQLApp.java` to query any other station.

## Troubleshooting

- **First `mvn compile` is slow:** Spark 4.1.1 pulls a large transitive dependency tree
  (Hadoop client, Jetty, Jersey, Scala...). This is a one-time download cached in `~/.m2`.
- **`Cannot access central ... in offline mode`:** run without `-o`/offline mode at least
  once so Maven can populate the local repository.
- **Garbled station names (`Ã©` instead of `é`):** make sure the CSV is read/saved as UTF-8;
  the file shipped here already is.
