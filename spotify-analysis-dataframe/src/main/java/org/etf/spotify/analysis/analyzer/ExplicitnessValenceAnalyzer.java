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
 * Analizator odnosa eksplicitnosti i valencije (muzička pozitivnost).
 */
public class ExplicitnessValenceAnalyzer implements Analyzer {

    // Broj opsega normaizovane popularnosti
    private static final int TOTAL_RANGES = 10;

    // Threshold za "dosljedan obrazac" (u koliko opsega se pojavljuje)
    private static final int CONSISTENCY_THRESHOLD = 7;  // 70% opsega

    @Override
    public void analyze(Dataset<Row> data) {

        // Normalizacija popularnosti kako bi se omogućilo grupisanje po intervalima širine 0.1
        Dataset<Row> dataNormalized = data.withColumn(
                "popularity_norm",
                col("popularity").divide(lit(100.0))
        );

        // Kreiranje opsega normalizovane popularnosti (0.0-0.1, 0.1-0.2, ..., 0.9-1.0)
        Dataset<Row> dataWithRanges = dataNormalized.withColumn(
                "popularity_range",
                when(col("popularity_norm").lt(0.1), "0.0-0.1")
                        .when(col("popularity_norm").lt(0.2), "0.1-0.2")
                        .when(col("popularity_norm").lt(0.3), "0.2-0.3")
                        .when(col("popularity_norm").lt(0.4), "0.3-0.4")
                        .when(col("popularity_norm").lt(0.5), "0.4-0.5")
                        .when(col("popularity_norm").lt(0.6), "0.5-0.6")
                        .when(col("popularity_norm").lt(0.7), "0.6-0.7")
                        .when(col("popularity_norm").lt(0.8), "0.7-0.8")
                        .when(col("popularity_norm").lt(0.9), "0.8-0.9")
                        .otherwise("0.9-1.0")
        );


        // Poređenje valencije eksplicitnih vs neeksplicitnih pesama
        Dataset<Row> comparison = dataWithRanges
                .groupBy("popularity_range")
                .agg(
                        // Prosečna valence eksplicitnih pesama u ovom opsegu
                        avg(when(col("explicit").equalTo(true), col("valence")))
                                .alias("explicit_valence"),

                        // Prosečna valence neeksplicitnih pesama u ovom opsegu
                        avg(when(col("explicit").equalTo(false), col("valence")))
                                .alias("non_explicit_valence"),

                        // Broj eksplicitnih pesama u ovom opsegu
                        count(when(col("explicit").equalTo(true), 1))
                                .alias("explicit_count"),

                        // Broj neeksplicitnih pesama u ovom opsegu
                        count(when(col("explicit").equalTo(false), 1))
                                .alias("non_explicit_count")
                )
                // Računanje razlike u valenciji
                .withColumn("valence_difference",
                        col("explicit_valence").minus(col("non_explicit_valence")))

                // Kategorizacija obrasca (koji tip pjesama je pozitivniji)
                .withColumn("pattern",
                        when(col("valence_difference").gt(0), "Explicit_More_Positive")
                                .when(col("valence_difference").lt(0), "NonExplicit_More_Positive")
                                .otherwise("Neutral"))

                // Sortiranje po opsegu (0-10, 10-20, ...)
                .orderBy("popularity_range");

        // Konverzija rezultata u JSON-friendly strukturu
        List<Map<String, Object>> rangeAnalysis = convertToList(comparison);

        // Analiza dosljednosti obrasca
        // Brojimo koliko puta se svaki obrazac pojavljuje
        Dataset<Row> patternCount = comparison
                .groupBy("pattern")
                .agg(count("*").alias("range_count"))
                .orderBy(col("range_count").desc());

        String dominantPattern = "";
        long dominantCount = 0;

        if (patternCount.count() > 0) {
            Row firstRow = patternCount.first();
            dominantPattern = firstRow.getString(0);
            dominantCount = firstRow.getAs("range_count");
        }

        // Obrazac je "dosljedan" ako se pojavljuje u 7+ opsega (70%)
        boolean isConsistent = dominantCount >= CONSISTENCY_THRESHOLD;

        // Izvoz rezultata u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Eksplicitnost vs Valencija Analiza",
                "Odnos između eksplicitnog sadržaja i muzičke pozitivnosti kroz nivoe popularnosti"
        );
        result.put("popularity_ranges_analysis", rangeAnalysis);
        result.put("pattern_consistency", Map.of(
                "dominant_pattern", dominantPattern,
                "appears_in_ranges", dominantCount,
                "is_consistent", isConsistent
        ));
        result.put("conclusion", isConsistent ?
                "Obrazac JE dosledan kroz većinu nivoa popularnosti" :
                "Obrazac NIJE dosledan, varira kroz nivoe popularnosti");

        ResultExporter.exportAnalysis(
                "07_explicitness_valence.json",
                result,
                "Eksplicitnost vs Valencija\n" +
                        "Analizirano opsega: " + TOTAL_RANGES + "\n" +
                        "Dominantan obrazac: " + dominantPattern + " (" + dominantCount + "/" + TOTAL_RANGES + ")\n" +
                        "Detalji: `07_explicitness_valence.json`"
        );
    }

    /**
     * Pomoćna metoda koja konvertuje Spark Dataset  sa rezultatima analize po opsezima popularnosti
     * u listu Map objekata, gde svaka mapa predstavlja jedan opseg.
     */
    private List<Map<String, Object>> convertToList(Dataset<Row> dataset) {
        List<Map<String, Object>> rangeAnalysis = new ArrayList<>();

        for (Row row : dataset.collectAsList()) {
            Map<String, Object> range = new LinkedHashMap<>();
            range.put("popularity_range", row.getString(0));
            range.put("explicit_valence", row.getAs("explicit_valence"));
            range.put("non_explicit_valence", row.getAs("non_explicit_valence"));
            range.put("valence_difference", row.getAs("valence_difference"));
            range.put("pattern", row.getAs("pattern"));
            range.put("explicit_count", row.getAs("explicit_count"));
            range.put("non_explicit_count", row.getAs("non_explicit_count"));
            rangeAnalysis.add(range);
        }

        return rangeAnalysis;
    }

}