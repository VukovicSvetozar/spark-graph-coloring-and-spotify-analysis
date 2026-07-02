package org.etf.spotify.analysis.config;

import org.apache.spark.sql.SparkSession;

/**
 * Utility klasa za konfiguraciju Apache Spark okruženja.
 */
public class SparkConfig {

    private static final String APP_NAME = "Spotify Data Analysis";

    private SparkConfig() {
    }

    /**
     * Kreira i konfiguriše SparkSession instancu sa optimizovanim podešavanjima.
     */
    public static SparkSession createSparkSession() {

        return SparkSession.builder()
                .appName(APP_NAME)
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .getOrCreate();
    }

}