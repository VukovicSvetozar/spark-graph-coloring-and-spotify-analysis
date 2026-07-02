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
 * Analizator "breakthrough" pjesama (popularnost >80) iz "slabog" albuma (prosjek <50) u odnosu na ostale pjesme iz datog albuma.
 */
public class BreakthroughSongsAnalyzer implements Analyzer {

    // Minimalna popularnost za breakthrough status
    private static final int BREAKTHROUGH_THRESHOLD = 80;

    // Maksimalna prosječna popularnost albuma
    private static final int ALBUM_AVG_THRESHOLD = 50;

    // Broj top pjesama za prikaz u rezultatima
    private static final int TOP_SONGS_DISPLAY = 20;

    @Override
    public void analyze(Dataset<Row> data) {

        // Identifikacija slabih albuma (prosječna popularnost <50)
        Dataset<Row> albumStats = data
                .groupBy("album_name")
                .agg(
                        avg("popularity").alias("avg_album_popularity"),
                        count("*").alias("track_count")
                )
                .filter(col("avg_album_popularity").lt(ALBUM_AVG_THRESHOLD));

        // Spajanje podataka o pjesmama sa album statistikom
        Dataset<Row> dataWithAlbumStats = data.join(albumStats, "album_name");

        // Filtriranje breakthrough pjesama (popularnost >80)
        Dataset<Row> breakthroughSongs = dataWithAlbumStats
                .filter(col("popularity").gt(BREAKTHROUGH_THRESHOLD))
                .select("track_name", "artists", "album_name", "popularity",
                        "avg_album_popularity", "energy", "danceability", "valence");

        long breakthroughCount = breakthroughSongs.count();

        // Konverzija top 20 breakthrough pjesama u JSON strukturu
        List<Map<String, Object>> songList = convertToSongList(breakthroughSongs);

        // Analiza
        AudioCharacteristics audioAnalysis = analyzeAudioCharacteristicsPerAlbum(
                dataWithAlbumStats, breakthroughCount);

        // Izvoz rezultata u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Breakthrough Pjesme Analiza",
                "Pjesme sa popularnošću >80 u albumima sa prosekom <50, upoređene sa ostalim pesmama unutar istog albuma"
        );
        result.put("criteria", Map.of(
                "song_popularity_threshold", BREAKTHROUGH_THRESHOLD,
                "album_avg_threshold", ALBUM_AVG_THRESHOLD
        ));
        result.put("breakthrough_songs_count", breakthroughCount);
        result.put("top_20_breakthrough_songs", songList);
        result.put("breakthrough_audio_characteristics", audioAnalysis.breakthroughAudio);
        result.put("other_songs_audio_characteristics", audioAnalysis.otherAudio);
        result.put("audio_differences", audioAnalysis.differences);
        result.put("analysis_method", "Poređenje Breakthrough pjesme sa ostalim pjesmama u okviru istog albuma.“");

        ResultExporter.exportAnalysis(
                "03_breakthrough_songs.json",
                result,
                "Breakthrough Pjesme\n" +
                        "Pronađeno breakthrough pjesama: " + breakthroughCount + "\n" +
                        "Razlika u energiji (vs ostale u albumu): " + String.format("%.3f",
                        audioAnalysis.differences.getOrDefault("energy_diff", 0.0)) + "\n" +
                        "Detalji: `03_breakthrough_songs.json`"
        );
    }

    /**
     * Konvertuje Spark Dataset sa breakthrough pjesmama u listu Map objekata.
     */
    private List<Map<String, Object>> convertToSongList(Dataset<Row> songs) {
        List<Map<String, Object>> songList = new ArrayList<>();

        for (Row row : songs.limit(TOP_SONGS_DISPLAY).collectAsList()) {
            Map<String, Object> song = new LinkedHashMap<>();
            song.put("track_name", row.getAs("track_name"));
            song.put("artists", row.getAs("artists"));
            song.put("album_name", row.getAs("album_name"));
            song.put("popularity", row.getAs("popularity"));
            song.put("album_avg_popularity", row.getAs("avg_album_popularity"));
            song.put("energy", row.getAs("energy"));
            song.put("danceability", row.getAs("danceability"));
            song.put("valence", row.getAs("valence"));
            songList.add(song);
        }

        return songList;
    }

    /*
        Za svaki album sa breakthrough pjesmom:
            - Nađi breakthrough pjesme u tom albumu
            - Nađi ostale pesme u tom albumu
            - Uporedi prosjeke
        Zatim prosjek svih razlika.
    */
    private AudioCharacteristics analyzeAudioCharacteristicsPerAlbum(
            Dataset<Row> allSongsInWeakAlbums,
            long breakthroughCount) {

        Map<String, Double> breakthroughAudio = new LinkedHashMap<>();
        Map<String, Double> otherAudio = new LinkedHashMap<>();
        Map<String, Double> differences = new LinkedHashMap<>();

        if (breakthroughCount > 0) {

            // Prosječne audio karakteristike breakthrough pjesama po albumima
            Dataset<Row> breakthroughPerAlbum = allSongsInWeakAlbums
                    .filter(col("popularity").gt(BREAKTHROUGH_THRESHOLD))
                    .groupBy("album_name")
                    .agg(
                            avg("energy").alias("breakthrough_energy"),
                            avg("danceability").alias("breakthrough_danceability"),
                            avg("valence").alias("breakthrough_valence")
                    );

            // Prosječne audio karakteristike ostalih pjesama po albumima
            Dataset<Row> otherPerAlbum = allSongsInWeakAlbums
                    .filter(col("popularity").leq(BREAKTHROUGH_THRESHOLD))
                    .groupBy("album_name")
                    .agg(
                            avg("energy").alias("other_energy"),
                            avg("danceability").alias("other_danceability"),
                            avg("valence").alias("other_valence")
                    );

            // join za dobijenje parova (breakthrough vs ostali) po albumu
            Dataset<Row> albumComparison = breakthroughPerAlbum
                    .join(otherPerAlbum, "album_name")
                    .withColumn("energy_diff",
                            col("breakthrough_energy").minus(col("other_energy")))
                    .withColumn("danceability_diff",
                            col("breakthrough_danceability").minus(col("other_danceability")))
                    .withColumn("valence_diff",
                            col("breakthrough_valence").minus(col("other_valence")));

            // Prosječne razlike preko svih albuma
            Row avgDifferences = albumComparison.agg(
                    avg("breakthrough_energy").alias("breakthrough_energy"),
                    avg("other_energy").alias("other_energy"),
                    avg("energy_diff").alias("energy_diff"),
                    avg("breakthrough_danceability").alias("breakthrough_danceability"),
                    avg("other_danceability").alias("other_danceability"),
                    avg("danceability_diff").alias("danceability_diff"),
                    avg("breakthrough_valence").alias("breakthrough_valence"),
                    avg("other_valence").alias("other_valence"),
                    avg("valence_diff").alias("valence_diff")
            ).first();

            // Popunjavanje mapa sa rezultatima
            breakthroughAudio.put("energy", avgDifferences.getAs("breakthrough_energy"));
            breakthroughAudio.put("danceability", avgDifferences.getAs("breakthrough_danceability"));
            breakthroughAudio.put("valence", avgDifferences.getAs("breakthrough_valence"));

            otherAudio.put("energy", avgDifferences.getAs("other_energy"));
            otherAudio.put("danceability", avgDifferences.getAs("other_danceability"));
            otherAudio.put("valence", avgDifferences.getAs("other_valence"));

            differences.put("energy_diff", avgDifferences.getAs("energy_diff"));
            differences.put("danceability_diff", avgDifferences.getAs("danceability_diff"));
            differences.put("valence_diff", avgDifferences.getAs("valence_diff"));
        }

        return new AudioCharacteristics(breakthroughAudio, otherAudio, differences);
    }

    /**
     * Pomoćna klasa koja enkapsulira rezultate audio analize.
     */
    private record AudioCharacteristics(Map<String, Double> breakthroughAudio, Map<String, Double> otherAudio,
                                        Map<String, Double> differences) {
    }

}