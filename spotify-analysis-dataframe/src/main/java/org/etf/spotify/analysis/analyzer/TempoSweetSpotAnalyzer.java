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
 * Analizator "sweet spot" tempa za popularne žanrove.
 */
public class TempoSweetSpotAnalyzer implements Analyzer {

    // Broj top žanrova za analizu
    private static final int TOP_GENRES_COUNT = 5;

    // Threshold za "značajan" uticaj sweet spot-a
    private static final double SIGNIFICANCE_THRESHOLD = 5.0;

    @Override
    public void analyze(Dataset<Row> data) {

        // Identifikacija top 5 najpopularnijih žanrova
        Dataset<Row> topGenres = data
                .groupBy("track_genre")
                .agg(
                        avg("popularity").alias("avg_popularity"),
                        count("*").alias("track_count")
                )
                .orderBy(col("avg_popularity").desc())
                .limit(TOP_GENRES_COUNT);

        List<Map<String, Object>> genreAnalyses = new ArrayList<>();

        // Za svaki od top 5 žanrova, analiziraj tempo raspone
        for (Row genreRow : topGenres.collectAsList()) {
            String genre = genreRow.getString(0);
            Double avgPop = genreRow.getAs("avg_popularity");

            // Filtriraj samo pjesme ovog žanra
            Dataset<Row> genreData = data.filter(col("track_genre").equalTo(genre));

            // Kategorizuj pjesme u tempo raspone
            // Koristi SQL CASE za performanse (bolje od nested when())
            Dataset<Row> tempoCategories = genreData.selectExpr(
                    "*",
                    """
                            CASE
                                WHEN tempo < 100 THEN 'Slow (<100 BPM)'
                                WHEN tempo >= 100 AND tempo <= 120 THEN 'Medium (100-120 BPM)'
                                WHEN tempo > 120 AND tempo <= 140 THEN 'Fast (121-140 BPM)'
                                ELSE 'Very Fast (>140 BPM)'
                            END as tempo_range
                            """
            );

            // Izračunaj statistiku za svaki tempo raspon
            Dataset<Row> tempoAnalysis = tempoCategories
                    .groupBy("tempo_range")
                    .agg(
                            avg("popularity").alias("avg_popularity"),
                            count("*").alias("track_count"),
                            avg("tempo").alias("avg_tempo")
                    )
                    .orderBy(col("avg_popularity").desc());

            // Identifikuj sweet spot i konvertuj rezultate
            TempoAnalysisResult analysisResult = analyzeTempoRanges(
                    tempoAnalysis, genre, avgPop);

            genreAnalyses.add(analysisResult.toMap());
        }

        // Globalna statistika i zaključak
        double avgAdvantage = calculateAverageAdvantage(genreAnalyses);

        // Izvoz rezultata u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Sweet Spot Tempo Analiza (2 boda)",
                "Optimalni rasponi tempa za 5 najpopularnijih žanrova"
        );
        result.put("top_5_genres_tempo_analysis", genreAnalyses);
        result.put("average_sweet_spot_advantage", avgAdvantage);
        result.put("conclusion", avgAdvantage > SIGNIFICANCE_THRESHOLD ?
                "Sweet spot tempo ima ZNAČAJAN uticaj na popularnost" :
                "Sweet spot tempo ima MINIMALAN uticaj na popularnost");

        ResultExporter.exportAnalysis(
                "04_tempo_sweetspot.json",
                result,
                "Sweet Spot Tempo Analiza\n" +
                        "Analizirano žanrova: " + TOP_GENRES_COUNT + "\n" +
                        "Sweet spot identifikovan za svaki žanr\n" +
                        "Prosječna prednost sweet spot-a: " + String.format("%.2f", avgAdvantage) + "\n" +
                        "Detalji: `04_tempo_sweetspot.json`"
        );
    }

    /**
     * Pomoćna metoda koja analizira tempo raspone za jedan žanr.
     * Identifikuje sweet spot (najpopularniji raspon) i računa prednost nad najlošijim rasponom.
     */
    private TempoAnalysisResult analyzeTempoRanges(Dataset<Row> tempoAnalysis,
                                                   String genre,
                                                   Double avgGenrePopularity) {
        List<Map<String, Object>> tempoRanges = new ArrayList<>();
        String sweetSpot = "";
        double sweetSpotPop = 0;

        // Konverzija svakog tempo raspona u Map
        for (Row tempoRow : tempoAnalysis.collectAsList()) {
            Map<String, Object> range = new LinkedHashMap<>();
            String rangeName = tempoRow.getString(0);
            Double rangePop = tempoRow.getAs("avg_popularity");
            Long rangeCount = tempoRow.getAs("track_count");
            Double avgTempo = tempoRow.getAs("avg_tempo");

            range.put("tempo_range", rangeName);
            range.put("avg_popularity", rangePop);
            range.put("avg_tempo", avgTempo);
            range.put("track_count", rangeCount);
            tempoRanges.add(range);

            // Prvi raspon (najviša popularnost) je sweet spot
            if (sweetSpot.isEmpty() && rangePop != null) {
                sweetSpot = rangeName;
                sweetSpotPop = rangePop;
            }
        }

        // Računanje prednosti sweet spot-a nad najlošijim rasponom
        Double sweetSpotAdvantage = null;
        if (tempoRanges.size() > 1) {
            double worstPop = tempoRanges.stream()
                    .mapToDouble(r -> (Double) r.get("avg_popularity"))
                    .min()
                    .orElse(0.0);
            sweetSpotAdvantage = sweetSpotPop - worstPop;
        }

        return new TempoAnalysisResult(
                genre, avgGenrePopularity, sweetSpot, sweetSpotPop,
                tempoRanges, sweetSpotAdvantage);
    }

    /**
     * Pomoćna metoda koja računa prosječnu prednost sweet spot-a preko svih žanrova.
     */
    private double calculateAverageAdvantage(List<Map<String, Object>> genreAnalyses) {
        return genreAnalyses.stream()
                .filter(g -> g.containsKey("sweet_spot_advantage"))
                .mapToDouble(g -> (Double) g.get("sweet_spot_advantage"))
                .average()
                .orElse(0.0);
    }

    /**
         * Pomoćna klasa koja enkapsulira rezultate analize tempa za jedan žanr.
         */
        private record TempoAnalysisResult(String genre, Double avgGenrePopularity, String sweetSpot,
                                           double sweetSpotPopularity, List<Map<String, Object>> tempoRanges,
                                           Double sweetSpotAdvantage) {

        /**
             * Konvertuje rezultat u Map strukturu za JSON export.
             */
            public Map<String, Object> toMap() {
                Map<String, Object> genreAnalysis = new LinkedHashMap<>();
                genreAnalysis.put("genre", genre);
                genreAnalysis.put("avg_genre_popularity", avgGenrePopularity);
                genreAnalysis.put("sweet_spot_tempo_range", sweetSpot);
                genreAnalysis.put("sweet_spot_popularity", sweetSpotPopularity);
                genreAnalysis.put("tempo_ranges_analysis", tempoRanges);

                if (sweetSpotAdvantage != null) {
                    genreAnalysis.put("sweet_spot_advantage", sweetSpotAdvantage);
                }

                return genreAnalysis;
            }

        }

}