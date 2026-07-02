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
 * Analizator dosljednosti umjetnika.
 * Cilj: Identifikovati umjetnike sa najdosljednijom popularnosti (niska standardna devijacija) i
 * ispitati vezu između dosljednosti i žanrovske specijalizacije.
 */
public class ArtistConsistencyAnalyzer implements Analyzer {

    // Minimalan broj pjesama da bi umjetnik bio uključen u analizu
    private static final int MIN_TRACKS = 5;

    // Minimalna prosječna popularnost (filter za nepoznate umjetnike)
    private static final double MIN_POPULARITY = 1.0;

    // Kategorije žanrovske raznolikosti
    private static final int HIGHLY_SPECIALIZED_MAX = 2;    // 1-2 žanra
    private static final int MODERATELY_DIVERSE_MAX = 5;    // 3-5 žanrova

    @Override
    public void analyze(Dataset<Row> data) {

        // Filtriranje solo umjetnika (bez kolaboracija)
        Dataset<Row> soloArtists = data.filter(not(col("artists").contains(";")));

        // Računanje dosljednosti (standardna devijacija popularnosti)
        Dataset<Row> artistConsistency = soloArtists
                .groupBy("artists")
                .agg(
                        stddev("popularity").alias("popularity_stddev"),
                        avg("popularity").alias("avg_popularity"),
                        count("*").alias("track_count")
                )
                .filter(col("track_count").geq(MIN_TRACKS))
                .filter(col("avg_popularity").gt(MIN_POPULARITY))
                .filter(col("popularity_stddev").gt(0))
                .orderBy(col("popularity_stddev"));

        // Top 20 najdosljednijih umjetnika
        Dataset<Row> mostConsistent = artistConsistency.limit(20);

        // Analiza žanrovske raznolikosti

        // Broj različitih žanrova po umjetniku
        Dataset<Row> artistGenreStats = soloArtists
                .groupBy("artists")
                .agg(
                        countDistinct("track_genre").alias("genre_count"),
                        first("track_genre").alias("primary_genre_candidate")
                );

        // Identifikacija primarnog žanra (najčešći žanr)
        Dataset<Row> artistGenreDetails = soloArtists
                .groupBy("artists", "track_genre")
                .agg(count("*").alias("genre_track_count"))
                .withColumn("rank", row_number().over(
                        org.apache.spark.sql.expressions.Window
                                .partitionBy("artists")
                                .orderBy(col("genre_track_count").desc())
                ))
                .filter(col("rank").equalTo(1))
                .select(
                        col("artists").alias("artist_for_primary"),
                        col("track_genre").alias("primary_genre"),
                        col("genre_track_count").alias("primary_genre_count")
                );

        // Spajanje svih statistika
        Dataset<Row> artistsWithStats = mostConsistent
                .join(artistGenreStats, "artists")
                .join(artistGenreDetails,
                        mostConsistent.col("artists").equalTo(artistGenreDetails.col("artist_for_primary")))
                .drop("artist_for_primary");

        // Kategorizacija umjetnika i priprema rezultata
        List<Map<String, Object>> consistentArtists = new ArrayList<>();

        int highlySpecialized = 0;
        int moderatelyDiverse = 0;
        int highlyDiverse = 0;

        for (Row artist : artistsWithStats.collectAsList()) {
            String artistName = artist.getAs("artists");
            Double stddev = artist.getAs("popularity_stddev");
            Double avgPop = artist.getAs("avg_popularity");
            Long trackCount = artist.getAs("track_count");
            Long genreCount = artist.getAs("genre_count");
            String primaryGenre = artist.getAs("primary_genre");
            Long primaryGenreCount = artist.getAs("primary_genre_count");

            double primaryGenrePercent = (primaryGenreCount * 100.0) / trackCount;

            Map<String, Object> artistData = new LinkedHashMap<>();
            artistData.put("artist", artistName);
            artistData.put("popularity_stddev", stddev);
            artistData.put("avg_popularity", avgPop);
            artistData.put("track_count", trackCount);
            artistData.put("genre_count", genreCount);
            artistData.put("primary_genre", primaryGenre);
            artistData.put("primary_genre_percentage", primaryGenrePercent);

            if (genreCount <= HIGHLY_SPECIALIZED_MAX) {
                artistData.put("diversity_category", "Highly_Specialized");
                highlySpecialized++;
            } else if (genreCount <= MODERATELY_DIVERSE_MAX) {
                artistData.put("diversity_category", "Moderately_Diverse");
                moderatelyDiverse++;
            } else {
                artistData.put("diversity_category", "Highly_Diverse");
                highlyDiverse++;
            }

            consistentArtists.add(artistData);
        }

        // Statistika i zaključak
        int total = highlySpecialized + moderatelyDiverse + highlyDiverse;
        double specializedPercentage = calculatePercentage(highlySpecialized, total);

        String conclusion = highlySpecialized > (total / 2) ?
                "Dosljednost JE povezana sa žanrovskom specijalizacijom" :
                "Dosljednost NIJE jako povezana sa žanrovskom specijalizacijom";

        // KORAK 7: Izvoz rezultata u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Dosljednost Umjetnika Analiza",
                "Umjetnici sa najnižom standardnom devijacijom popularnosti i njihova žanrovska raznolikost"
        );
        result.put("top_20_most_consistent_artists", consistentArtists);
        result.put("diversity_statistics", Map.of(
                "highly_specialized_1_2_genres", highlySpecialized,
                "moderately_diverse_3_5_genres", moderatelyDiverse,
                "highly_diverse_6plus_genres", highlyDiverse,
                "highly_specialized_percentage", specializedPercentage
        ));
        result.put("conclusion", conclusion);

        ResultExporter.exportAnalysis(
                "08_artist_consistency.json",
                result,
                "Dosljednost Umjetnika (2 boda)\n" +
                        "Analizirano umjetnika: 20\n" +
                        "Visoko specijalizovanih: " + highlySpecialized + " (" +
                        String.format("%.1f%%", specializedPercentage) + ")\n" +
                        "Detalji: `08_artist_consistency.json`"
        );
    }

    /**
     * Pomoćna metoda za računanje procenta.
     */
    private double calculatePercentage(int part, int total) {
        if (total == 0) {
            return 0.0;
        }
        return (part * 100.0) / total;
    }

}