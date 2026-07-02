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
 * Analizator akustičnih/instrumentalnih vs vokalnih žanrova.
 */
public class AcousticInstrumentalAnalyzer implements Analyzer {

    // Threshold za "visoke" vrednosti (akustične/instrumentalne pesme)
    private static final double HIGH_THRESHOLD = 0.8;

    // Threshold za "niske" vrednosti (vokalne pesme)
    private static final double LOW_THRESHOLD = 0.3;

    // Minimalna razlika u popularnosti koja se smatra značajnom
    private static final double SIGNIFICANCE_THRESHOLD = 5.0;

    @Override
    public void analyze(Dataset<Row> data) {

        // Filtriraj pjesme sa visokim instrumentalnim vrijednostima
        Dataset<Row> highAcousticInstrumental = data
                .filter(col("acousticness").gt(HIGH_THRESHOLD)
                        .and(col("instrumentalness").gt(HIGH_THRESHOLD)));

        // Grupiši instrumentalne pjesme po žanru
        Dataset<Row> acousticInstrumentalGenres = highAcousticInstrumental
                .groupBy("track_genre")
                .agg(
                        avg("popularity").alias("avg_popularity"),
                        avg("acousticness").alias("avg_acousticness"),
                        avg("instrumentalness").alias("avg_instrumentalness"),
                        count("*").alias("track_count")
                )
                .orderBy(col("avg_popularity").desc());

        // Filtriraj pjesme sa niskim vrijednostima (vokalne)
        Dataset<Row> lowAcousticInstrumental = data
                .filter(col("acousticness").lt(LOW_THRESHOLD)
                        .and(col("instrumentalness").lt(LOW_THRESHOLD)));

        // Grupiši vokalne pjesme po žanru
        Dataset<Row> vocalGenres = lowAcousticInstrumental
                .groupBy("track_genre")
                .agg(
                        avg("popularity").alias("avg_popularity"),
                        avg("acousticness").alias("avg_acousticness"),
                        avg("instrumentalness").alias("avg_instrumentalness"),
                        count("*").alias("track_count")
                )
                .orderBy(col("avg_popularity").desc());

        // Prebrojavanje žanrova u svakoj kategoriji
        long acousticCount = acousticInstrumentalGenres.count();
        long vocalCount = vocalGenres.count();

        // Konvertovanje Spark Row objekata u Java Liste
        List<Map<String, Object>> acousticList = convertToList(acousticInstrumentalGenres);
        List<Map<String, Object>> vocalList = convertToList(vocalGenres);

        // Poređenje prosječne popularnosti
        Double acousticPopularity = null;
        Double vocalPopularity = null;
        Double difference = null;
        String conclusion = "Nedovoljno podataka za zaključak";

        // Računamo razliku samo ako obe kategorije imaju žanrove
        if (acousticCount > 0 && vocalCount > 0) {
            // Prosječna popularnost instrumentalnih žanrova
            Row aStats = acousticInstrumentalGenres
                    .agg(avg("avg_popularity"))
                    .first();
            acousticPopularity = aStats.getAs(0);

            // Prosječna popularnost vokalnih žanrova
            Row vStats = vocalGenres
                    .agg(avg("avg_popularity"))
                    .first();
            vocalPopularity = vStats.getAs(0);

            // Računanje razlike i zaključka
            if (acousticPopularity != null && vocalPopularity != null) {
                difference = acousticPopularity - vocalPopularity;

                if (difference > SIGNIFICANCE_THRESHOLD) {
                    conclusion = "Instrumentalni fokus ima POZITIVAN uticaj na komercijalni uspjeh";
                } else if (difference < -SIGNIFICANCE_THRESHOLD) {
                    conclusion = "Instrumentalni fokus ima NEGATIVAN uticaj na komercijalni uspjeh";
                } else {
                    conclusion = "Instrumentalni fokus ima NEUTRALAN uticaj na komercijalni uspjeh";
                }
            }
        }

        // KORAK 7: Export rezultata u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Akustične vs Vokalne Pjesme Analiza",
                "Upoređivanje žanrova sa visokim akustičnim/instrumentalnim vrijednostima i vokalnih žanrova"
        );
        result.put("acoustic_instrumental_genres", acousticList);
        result.put("acoustic_instrumental_count", acousticCount);
        result.put("acoustic_instrumental_avg_popularity", acousticPopularity);
        result.put("vocal_heavy_genres", vocalList);
        result.put("vocal_heavy_count", vocalCount);
        result.put("vocal_heavy_avg_popularity", vocalPopularity);
        result.put("popularity_difference", difference);
        result.put("conclusion", conclusion);

        ResultExporter.exportAnalysis(
                "09_acoustic_vs_vocal.json",
                result,
                "Akustične vs Vokalne\n" +
                        "Akustični/instrumentalni žanrova: " + acousticCount + "\n" +
                        "Vokalnih žanrova: " + vocalCount + "\n" +
                        "Razlika popularnosti: " + (difference != null ? String.format("%.2f", difference) : "N/A") + "\n" +
                        "Detalji: `09_acoustic_vs_vocal.json`"
        );
    }

    /**
     * Pomoćna metoda koja konvertuje Spark Row objekte u Map strukturu koja se može lako serijalizovati u JSON format.
     */
    private List<Map<String, Object>> convertToList(Dataset<Row> dataset) {
        List<Map<String, Object>> resultList = new ArrayList<>();

        for (Row row : dataset.collectAsList()) {
            Map<String, Object> genre = new LinkedHashMap<>();
            genre.put("genre", row.getAs("track_genre"));
            genre.put("avg_acousticness", row.getAs("avg_acousticness"));
            genre.put("avg_instrumentalness", row.getAs("avg_instrumentalness"));
            genre.put("avg_popularity", row.getAs("avg_popularity"));
            genre.put("track_count", row.getAs("track_count"));
            resultList.add(genre);
        }

        return resultList;
    }

}