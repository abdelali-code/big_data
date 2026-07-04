package com.atelier.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Practical Activity: Spark SQL - bike-sharing dataset.
 *
 * Reproduces every question of the TP (Exercise 1) against bike_sharing.csv:
 * 1. Data Loading & Exploration
 * 2. Temporary View
 * 3. Basic SQL Queries
 * 4. Aggregation Queries
 * 5. Time-Based Analysis
 * 6. User Behavior Analysis
 *
 * Run with: mvn compile exec:java   (see README.md)
 * or:       spark-submit --class com.atelier.sparksql.SparkSQLApp target/spark-sql-atelier.jar [path/to/bike_sharing.csv]
 */
public class SparkSQLApp {

    // The dataset actually used here has real station names (no station literally called
    // "Station A"). We reuse the most frequent start station for question 3.2 so the query
    // returns rows; swap this constant to test any other station.
    private static final String SAMPLE_STATION = "Station Centre-Ville";

    public static void main(String[] args) {
        String csvPath = args.length > 0 ? args[0] : "data/bike_sharing.csv";

        SparkSession spark = SparkSession.builder()
                .appName("Bike Sharing - Spark SQL Atelier")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // ---------------------------------------------------------------
        // 1. Data Loading & Exploration
        // ---------------------------------------------------------------
        section("1. Data Loading & Exploration");

        StructType schema = new StructType(new StructField[]{
                new StructField("rental_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("user_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("gender", DataTypes.StringType, true, Metadata.empty()),
                new StructField("start_time", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("end_time", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("start_station", DataTypes.StringType, true, Metadata.empty()),
                new StructField("end_station", DataTypes.StringType, true, Metadata.empty()),
                new StructField("duration_minutes", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("price", DataTypes.DoubleType, true, Metadata.empty())
        });

        Dataset<Row> bikeRentals = spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(csvPath);

        System.out.println("-- 1.2 Schema --");
        bikeRentals.printSchema();

        System.out.println("-- 1.3 First 5 rows --");
        bikeRentals.show(5, false);

        System.out.println("-- 1.4 Total number of rentals --");
        long total = bikeRentals.count();
        System.out.println("Total rentals: " + total);

        // ---------------------------------------------------------------
        // 2. Create a Temporary View
        // ---------------------------------------------------------------
        section("2. Create a Temporary View");
        bikeRentals.createOrReplaceTempView("bike_rentals_view");
        System.out.println("View 'bike_rentals_view' created.");

        // ---------------------------------------------------------------
        // 3. Basic SQL Queries
        // ---------------------------------------------------------------
        section("3. Basic SQL Queries");

        System.out.println("-- 3.1 Rentals longer than 30 minutes --");
        spark.sql("SELECT * FROM bike_rentals_view WHERE duration_minutes > 30")
                .show(20, false);

        System.out.println("-- 3.2 Rentals starting at \"" + SAMPLE_STATION + "\" --");
        spark.sql(
                "SELECT * FROM bike_rentals_view WHERE start_station = '" + SAMPLE_STATION + "'"
        ).show(20, false);

        System.out.println("-- 3.3 Total revenue --");
        spark.sql("SELECT ROUND(SUM(price), 2) AS total_revenue FROM bike_rentals_view")
                .show(false);

        // ---------------------------------------------------------------
        // 4. Aggregation Queries
        // ---------------------------------------------------------------
        section("4. Aggregation Queries");

        System.out.println("-- 4.1 Rentals per start station --");
        Dataset<Row> rentalsPerStation = spark.sql(
                "SELECT start_station, COUNT(*) AS nb_rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station " +
                        "ORDER BY nb_rentals DESC"
        );
        rentalsPerStation.show(50, false);

        System.out.println("-- 4.2 Average rental duration per start station --");
        spark.sql(
                "SELECT start_station, ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station " +
                        "ORDER BY avg_duration_minutes DESC"
        ).show(50, false);

        System.out.println("-- 4.3 Station with the highest number of rentals --");
        rentalsPerStation.limit(1).show(false);

        // ---------------------------------------------------------------
        // 5. Time-Based Analysis
        // ---------------------------------------------------------------
        section("5. Time-Based Analysis");

        System.out.println("-- 5.1 Hour extracted from start_time (sample) --");
        spark.sql(
                "SELECT rental_id, start_time, HOUR(start_time) AS start_hour " +
                        "FROM bike_rentals_view"
        ).show(10, false);

        System.out.println("-- 5.2 Rentals per hour (peak hours) --");
        spark.sql(
                "SELECT HOUR(start_time) AS start_hour, COUNT(*) AS nb_rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY HOUR(start_time) " +
                        "ORDER BY nb_rentals DESC"
        ).show(24, false);

        System.out.println("-- 5.3 Most popular start station in the morning (7-12) --");
        spark.sql(
                "SELECT start_station, COUNT(*) AS nb_rentals " +
                        "FROM bike_rentals_view " +
                        "WHERE HOUR(start_time) BETWEEN 7 AND 12 " +
                        "GROUP BY start_station " +
                        "ORDER BY nb_rentals DESC"
        ).show(10, false);

        // ---------------------------------------------------------------
        // 6. User Behavior Analysis
        // ---------------------------------------------------------------
        section("6. User Behavior Analysis");

        System.out.println("-- 6.1 Average age of users --");
        spark.sql("SELECT ROUND(AVG(age), 2) AS avg_age FROM bike_rentals_view").show(false);

        System.out.println("-- 6.2 Users by gender --");
        spark.sql(
                "SELECT gender, COUNT(DISTINCT user_id) AS nb_users, COUNT(*) AS nb_rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY gender " +
                        "ORDER BY nb_rentals DESC"
        ).show(false);

        System.out.println("-- 6.3 Rentals by age group --");
        spark.sql(
                "SELECT CASE " +
                        "  WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                        "  WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                        "  WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                        "  WHEN age >= 51 THEN '51+' " +
                        "  ELSE 'under 18' " +
                        "END AS age_group, " +
                        "COUNT(*) AS nb_rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY age_group " +
                        "ORDER BY nb_rentals DESC"
        ).show(false);

        spark.stop();
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("========================================================");
        System.out.println(title);
        System.out.println("========================================================");
    }
}
