package org.etf.spotify.analysis.analyzer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.etf.spotify.analysis.exporter.ResultExporter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * Analizator korelacije između eksplicitnog sadržaja i popularnosti,
 * Istražuje se u kojim žanrovima eksplicitni sadržaj povećava/smanjuje popularnost.
 */
public class ExplicitContentAnalyzer implements Analyzer {

    // Broj top žanrova za pozitivan/negativan uticaj
    private static final int TOP_GENRES_LIMIT = 10;

    // Threshold za "značajnu" razliku u popularnosti (samo razlike >5% su stvarno značajne)
    private static final double SIGNIFICANCE_THRESHOLD = 5.0;

    @Override
    public void analyze(Dataset<Row> data) {

        // Prosječna popularnost eksplicitnih pjesama po žanrovima
        Dataset<Row> explicitStats = data
                .filter(col("explicit").equalTo(true))
                .groupBy("track_genre")
                .agg(avg("popularity").alias("explicit_popularity"))
                .withColumnRenamed("track_genre", "genre_explicit");

        // Prosječna popularnost neeksplicitnih pjesama po žanrovima
        Dataset<Row> nonExplicitStats = data
                .filter(col("explicit").equalTo(false))
                .groupBy("track_genre")
                .agg(avg("popularity").alias("non_explicit_popularity"))
                .withColumnRenamed("track_genre", "genre_non_explicit");

        /*
            Spajanje rezultata
            Sa FULL OUTER JOIN uključili bi i samo eksplicitne i samo neeksplicitne žanrove
                pa možemo imati lažno pozitivne ili negativne rezultate
            Sa INNER JOIN zadržavaju se samo žanrovi koji imaju i eksplicitne i neeksplicitne pjesme.
         */
        Dataset<Row> correlationAnalysis = explicitStats
                .join(nonExplicitStats,
                        explicitStats.col("genre_explicit").equalTo(nonExplicitStats.col("genre_non_explicit")),
                        "inner")
                .withColumn("track_genre", col("genre_explicit"))
                .drop("genre_explicit", "genre_non_explicit")
                .withColumn("popularity_difference",
                        col("explicit_popularity").minus(col("non_explicit_popularity")))
                .withColumn("explicit_impact",
                        when(col("popularity_difference").gt(SIGNIFICANCE_THRESHOLD), "Positive")
                                .when(col("popularity_difference").lt(-SIGNIFICANCE_THRESHOLD), "Negative")
                                .otherwise("Neutral"))
                .orderBy(col("popularity_difference").desc());

        // Konverzija rezultata u strukturu prilagođenu JSON formatu
        List<Map<String, Object>> genreCorrelations = convertToGenreList(correlationAnalysis);

        // Statistika - brojanje žanrova po kategorijama
        long positiveCount = correlationAnalysis.filter(col("explicit_impact").equalTo("Positive")).count();
        long negativeCount = correlationAnalysis.filter(col("explicit_impact").equalTo("Negative")).count();
        long neutralCount = correlationAnalysis.filter(col("explicit_impact").equalTo("Neutral")).count();

        // Identifikacija top 10 pozitivnih i negativnih žanrova
        List<Map<String, Object>> topPositive = extractTopGenres(
                correlationAnalysis, "Positive", true);
        List<Map<String, Object>> topNegative = extractTopGenres(
                correlationAnalysis, "Negative", false);

        // Izvoz rezultata u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Eksplicitni Sadržaj Korelacija",
                "Uticaj eksplicitnog sadržaja na popularnost po žanrovima"
        );
        result.put("significance_threshold", SIGNIFICANCE_THRESHOLD);
        result.put("join_method", "INNER (samo žanrovi sa i eksplicitnim i neeksplicitnim pjesmama)");
        result.put("all_genres_correlation", genreCorrelations);
        result.put("impact_statistics", Map.of(
                "positive_impact_genres", positiveCount,
                "negative_impact_genres", negativeCount,
                "neutral_impact_genres", neutralCount
        ));
        result.put("top_10_positive_impact_genres", topPositive);
        result.put("top_10_negative_impact_genres", topNegative);

        ResultExporter.exportAnalysis(
                "05_explicit_content_correlation.json",
                result,
                "Eksplicitni Sadržaj Korelacija\n" +
                        "Pozitivan uticaj u " + positiveCount + " žanrova\n" +
                        "Negativan uticaj u " + negativeCount + " žanrova\n" +
                        "Neutralan uticaj u " + neutralCount + " žanrova\n" +
                        "Detalji: `05_explicit_content_correlation.json`"
        );
    }

    /**
     * Pomoćna metoda koja konvertuje Spark Dataset sa podacima o korelaciji po žanrovima
     * u listu Map objekata, gdje svaka mapa predstavlja jedan žanr.
     */
    private List<Map<String, Object>> convertToGenreList(Dataset<Row> dataset) {
        List<Map<String, Object>> genreCorrelations = new ArrayList<>();

        for (Row row : dataset.collectAsList()) {
            Map<String, Object> genreData = new LinkedHashMap<>();
            genreData.put("genre", row.getAs("track_genre"));
            genreData.put("explicit_popularity", row.getAs("explicit_popularity"));
            genreData.put("non_explicit_popularity", row.getAs("non_explicit_popularity"));
            genreData.put("popularity_difference", row.getAs("popularity_difference"));
            genreData.put("explicit_impact", row.getAs("explicit_impact"));
            genreCorrelations.add(genreData);
        }

        return genreCorrelations;
    }

    /**
     * Pomoćna metoda koja ekstraktuje top 10 žanrova sa pozitivnim ili negativnim uticajem.
     */
    private List<Map<String, Object>> extractTopGenres(Dataset<Row> dataset, String impactType, boolean descending) {
        List<Map<String, Object>> topGenres = new ArrayList<>();

        Dataset<Row> filtered = dataset
                .filter(col("explicit_impact").equalTo(impactType));

        Dataset<Row> sorted = descending ?
                filtered.orderBy(col("popularity_difference").desc()) :
                filtered.orderBy(col("popularity_difference").asc());

        for (Row row : sorted.limit(TOP_GENRES_LIMIT).collectAsList()) {
            Map<String, Object> genre = new LinkedHashMap<>();
            genre.put("genre", row.getAs("track_genre"));

            // Različito imenovanje za pozitivne vs negativne
            String diffKey = impactType.equals("Positive") ?
                    "popularity_boost" : "popularity_penalty";
            genre.put(diffKey, row.getAs("popularity_difference"));

            topGenres.add(genre);
        }

        return topGenres;
    }

}