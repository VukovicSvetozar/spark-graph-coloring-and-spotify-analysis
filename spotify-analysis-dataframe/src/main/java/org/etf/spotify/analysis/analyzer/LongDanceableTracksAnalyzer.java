package org.etf.spotify.analysis.analyzer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.etf.spotify.analysis.exporter.ResultExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

/**
 * Analizator dugih pjesama sa visokom plesivošću (2 boda).
 */
public class LongDanceableTracksAnalyzer implements Analyzer {

    private static final Logger logger = LoggerFactory.getLogger(LongDanceableTracksAnalyzer.class);

    // Minimalna plesivost za uključivanje pjesme u analizu (0.8 = vrlo plesljiva)
    private static final double DANCEABILITY_THRESHOLD = 0.8;

    // Konverzija milisekundi u minute
    private static final int MS_TO_MINUTES = 60000;

    // Broj najdužih pjesama koje analiziramo
    private static final int TOP_TRACKS_LIMIT = 10;

    @Override
    public void analyze(Dataset<Row> data) {

        // Filtriranje i identifikacija dugih plesnih pjesama
        // Pronalazi top 10 najdužih pjesama sa plesivošću > 0.8
        Dataset<Row> longDanceableTracks = data
                .filter(col("danceability").isNotNull())
                .filter(col("duration_ms").isNotNull())
                .filter(col("track_genre").isNotNull())
                .filter(col("popularity").isNotNull())
                .filter(col("danceability").gt(DANCEABILITY_THRESHOLD))
                .withColumn("duration_minutes", col("duration_ms").divide(MS_TO_MINUTES))
                .orderBy(col("duration_ms").desc())
                .limit(TOP_TRACKS_LIMIT);

        long count = longDanceableTracks.count();

        // Izračunavanje žanrovskih prosječnih vrijednosti
        // Preračunavamo statistiku za sve žanrove odjednom (optimizacija)
        // Ovo je brže od višestrukih filter operacija u petlji
        Dataset<Row> allGenreStats = data
                .groupBy("track_genre")
                .agg(
                        avg("popularity").alias("genre_avg_popularity"),
                        avg("duration_ms").alias("genre_avg_duration_ms")
                );

        // Spajanje podataka o pjesmama sa žanrovskim statistikama
        Dataset<Row> tracksWithGenreStats = longDanceableTracks
                .join(allGenreStats, "track_genre");

        // Konverzija rezultata u strukturu prilagođenu JSON formatu
        List<Map<String, Object>> tracksList = convertToList(tracksWithGenreStats);

        // Analiza ukupnog trenda
        // Upoređujemo prosječnu popularnost dugih plesnih pjesama sa prosječnom popularnošću svih plesnih pjesama
        Row longDanceStats = longDanceableTracks
                .agg(avg("popularity"), avg("duration_minutes"), avg("danceability"))
                .first();

        Row allDanceStats = data
                .filter(col("danceability").gt(DANCEABILITY_THRESHOLD))
                .agg(avg("popularity"), avg("duration_ms"))
                .first();

        Double longPop = longDanceStats.getAs(0);
        Double allPop = allDanceStats.getAs(0);

        // Izvoz rezultata u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Duge Plesne Pjesme Analiza",
                "10 najdužih pjesama sa plesivošću >0.8 i njihov komercijalni uspjeh"
        );
        result.put("danceability_threshold", DANCEABILITY_THRESHOLD);
        result.put("top_10_long_danceable_tracks", tracksList);

        Map<String, Object> overallStats = new LinkedHashMap<>();
        overallStats.put("long_danceable_avg_popularity", longPop != null ? longPop : 0.0);
        overallStats.put("all_danceable_avg_popularity", allPop != null ? allPop : 0.0);
        overallStats.put("length_impact", longPop != null && allPop != null ? longPop - allPop : 0.0);

        result.put("overall_statistics", overallStats);
        result.put("conclusion", longPop != null && allPop != null && (longPop - allPop) > 0 ?
                "Duže plesne pjesme imaju POZITIVAN uticaj na popularnost" :
                "Duže plesne pjesme imaju NEGATIVAN uticaj na popularnost");

        ResultExporter.exportAnalysis(
                "06_long_danceable_tracks.json",
                result,
                "Duge Plesne Pjesme (2 boda)\n" +
                        "Analizirano pjesama: " + count + "\n" +
                        "Uticaj dužine: " + String.format("%.2f",
                        longPop != null && allPop != null ? longPop - allPop : 0) + "\n" +
                        "Detalji: `06_long_danceable_tracks.json`"
        );
    }

    /**
     * Pomoćna metoda koja konvertuje Spark Dataset sa podacima o pjesmama i njihovim žanrovskim statistikama
     * u listu Map objekata, gdje svaki Map predstavlja jednu pjesmu.
     */
    private List<Map<String, Object>> convertToList(Dataset<Row> dataset) {
        List<Map<String, Object>> tracksList = new ArrayList<>();

        for (Row track : dataset.collectAsList()) {
            try {
                String trackName = track.getAs("track_name");
                String artists = track.getAs("artists");
                String genre = track.getAs("track_genre");
                Double durationMin = track.getAs("duration_minutes");
                Float danceability = track.getAs("danceability");
                Object popObj = track.getAs("popularity");
                Double genreAvgPop = track.getAs("genre_avg_popularity");
                Double genreAvgDurMs = track.getAs("genre_avg_duration_ms");

                if (durationMin != null && popObj != null) {
                    // Konverzija popularity u Double (može biti Integer ili Double)
                    double popularity = popObj instanceof Integer ?
                            ((Integer) popObj).doubleValue() : (Double) popObj;

                    Map<String, Object> trackData = new LinkedHashMap<>();
                    trackData.put("track_name", trackName);
                    trackData.put("artists", artists);
                    trackData.put("genre", genre);
                    trackData.put("duration_minutes", durationMin);
                    trackData.put("danceability", danceability);
                    trackData.put("popularity", popularity);
                    trackData.put("genre_avg_popularity", genreAvgPop);
                    trackData.put("genre_avg_duration_minutes",
                            genreAvgDurMs != null ? genreAvgDurMs / MS_TO_MINUTES : null);
                    trackData.put("popularity_vs_genre",
                            genreAvgPop != null ? popularity - genreAvgPop : null);

                    tracksList.add(trackData);
                }
            } catch (Exception e) {
                logger.warn("Greška pri analizi pjesme: {}", e.getMessage());
            }
        }

        return tracksList;
    }

}