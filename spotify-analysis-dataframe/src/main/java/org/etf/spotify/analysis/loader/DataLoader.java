package org.etf.spotify.analysis.loader;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * Klasa odgovorna za učitavanje i validaciju Spotify skupa podataka.
 * Odgovornosti:
 * - Definisanje eksplicitne šeme podataka
 * - Učitavanje CSV fajla sa validacijom strukture
 * - Validacija kvaliteta podataka (provjera null vrijednosti, provjera opsega)
 * - Čišćenje podataka (uklanjanje redova sa null vrijednostima)
 */
@SuppressWarnings("ClassCanBeRecord")
public class DataLoader {

    private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);
    /**
     * Audio karakteristike koje moraju biti u opsegu [0.0, 1.0].
     */
    private static final String[] AUDIO_FEATURES = {
            "danceability",
            "energy",
            "acousticness",
            "instrumentalness",
            "valence"
    };
    private final SparkSession spark;

    public DataLoader(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Kreira eksplicitnu šemu za Spotify dataset.
     * NULLABLE STRATEGIJA:
     * - false: Obavezne kolone potrebne za analize
     * - true: Opcione kolone koje mogu nedostajati
     */
    private StructType createSpotifySchema() {
        return new StructType()
                .add("_empty", DataTypes.IntegerType, true)             // Redni broj
                .add("track_id", DataTypes.StringType, false)           // Jedinstveni ID
                .add("artists", DataTypes.StringType, false)            // Izvođač(i)
                .add("album_name", DataTypes.StringType, true)          // Album (opciono)
                .add("track_name", DataTypes.StringType, false)         // Naziv pjesme
                .add("popularity", DataTypes.IntegerType, false)        // Popularnost [0-100)
                .add("duration_ms", DataTypes.IntegerType, false)       // Trajanje u ms
                .add("explicit", DataTypes.BooleanType, false)          // Eksplicitna sadržaj
                .add("danceability", DataTypes.FloatType, false)        // Plesivost [0.0, 1.0]
                .add("energy", DataTypes.FloatType, false)              // Energija [0.0, 1.0]
                .add("key", DataTypes.IntegerType, true)                // Muzicki tonalitet [-1, 11]
                .add("loudness", DataTypes.FloatType, true)             // Glasnoća (dB)
                .add("mode", DataTypes.IntegerType, true)               // Mol/dur (0/1)
                .add("speechiness", DataTypes.FloatType, true)          // Govorljivost [0.0, 1.0]
                .add("acousticness", DataTypes.FloatType, false)        // Akustičnost [0.0, 1.0]
                .add("instrumentalness", DataTypes.FloatType, false)    // Instrumentalnost [0.0, 1.0]
                .add("liveness", DataTypes.FloatType, true)             // Uživo snimak [0.0, 1.0]
                .add("valence", DataTypes.FloatType, false)             // Pozitivnost [0.0, 1.0]
                .add("tempo", DataTypes.FloatType, false)               // Brzina izvođenja [50, 200]
                .add("time_signature", DataTypes.IntegerType, true)     // Takt [3, 7]
                .add("track_genre", DataTypes.StringType, false);       // Žanr
    }

    /**
     * Učitava Spotify CSV dataset sa eksplicitnom šemom i izvršava validaciju.
     */
    public Dataset<Row> loadData(String path) {
        logger.info("Učitavanje podataka iz: {}", path);

        // Kreiraj eksplicitnu šemu
        StructType schema = createSpotifySchema();
        logger.info("Kreirana eksplicitna šema sa {} kolona", schema.fields().length);

        // Učitaj podatke sa eksplicitnom šemom
        Dataset<Row> data = spark.read()
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .option("sep", ",")
                .option("mode", "DROPMALFORMED")
                .load(path);

        logger.info("CSV učitan sa {} kolona", data.columns().length);

        // Ukloni sistemsku kolonu (prva kolona)
        data = data.drop("_empty");
        logger.info("Broj kolona nakon uklanjanja prazne kolone: {}", data.columns().length);

        long totalRows = data.count();
        logger.info("Ukupno redova učitano: {}", totalRows);

        if (totalRows == 0) {
            throw new IllegalStateException("Dataset je prazan!");
        }

        // Validacija
        validateData(data);

        // Čiščenje podataka

        // Obriši redove sa null vrijednostima
        String[] criticalColumns = {
                "track_id", "artists", "track_name", "track_genre",
                "popularity", "duration_ms", "danceability", "energy",
                "valence", "acousticness", "instrumentalness"
        };
        data = data.na().drop("any", criticalColumns);
        long afterNullCleaning = data.count();
        logger.info("Nakon uklanjanja null vrijednosti: {} redova", afterNullCleaning);

        // Obriši redove sa lošim audio vrijednostima
        logger.info("Filtriranje redova sa audio karakteristikama van opsega [0.0, 1.0]...");

        // ⚠️ DEBUG: Broj loših redova PRE filtera
        long badAudioBefore = data.filter(
                col("danceability").cast(DataTypes.DoubleType).lt(0.0)
                        .or(col("danceability").cast(DataTypes.DoubleType).gt(1.0))
                        .or(col("energy").cast(DataTypes.DoubleType).lt(0.0))
                        .or(col("energy").cast(DataTypes.DoubleType).gt(1.0))
                        .or(col("acousticness").cast(DataTypes.DoubleType).lt(0.0))
                        .or(col("acousticness").cast(DataTypes.DoubleType).gt(1.0))
                        .or(col("instrumentalness").cast(DataTypes.DoubleType).lt(0.0))
                        .or(col("instrumentalness").cast(DataTypes.DoubleType).gt(1.0))
                        .or(col("valence").cast(DataTypes.DoubleType).lt(0.0))
                        .or(col("valence").cast(DataTypes.DoubleType).gt(1.0))
        ).count();
        logger.info("DEBUG: Broj redova sa lošim audio vrednostima PRE filtera: {}", badAudioBefore);


        data = filterValidAudioFeatures(data);
        long cleanRows = data.count();
        long totalRemoved = totalRows - cleanRows;
        long audioRemoved = afterNullCleaning - cleanRows;

        logger.info("Broj redova nakon čišćenja: {} (uklonjeno: {})",
                cleanRows, totalRemoved);
        logger.info("  - Uklonjeno zbog null: {}", totalRows - afterNullCleaning);
        logger.info("  - Uklonjeno zbog loših audio vrednosti: {}", audioRemoved);

        return data;
    }

    /**
     * Validacija kvaliteta učitanih podataka i provjera tipova.
     * Izvršava tri vrste provjera:
     * - Provjera praznog dataseta
     * - Provjera null vrijednosti u ključnim kolonama
     * - Provjera raspona audio karakteristika
     * Ova metoda samo loguje upozorenja.
     */
    private void validateData(Dataset<Row> data) {
        long totalRows = data.count();

        if (totalRows == 0) {
            logger.error("GREŠKA: Dataset je prazan!");
            return;
        }

        logger.info("Validacija podataka...");

        long nullTrackIds = data.filter(data.col("track_id").isNull()).count();
        long nullArtists = data.filter(data.col("artists").isNull()).count();
        long nullGenres = data.filter(data.col("track_genre").isNull()).count();

        logger.info("Ukupno redova: {}", totalRows);
        logger.info("Redova sa null track_id: {}", nullTrackIds);
        logger.info("Redova sa null artists: {}", nullArtists);
        logger.info("Redova sa null track_genre: {}", nullGenres);

        validateAudioFeatures(data);
    }

    /**
     * Validacija audio karakteristika u ispravnom rasponu [0.0, 1.0].
     * Metoda ne mijenja podatke - samo loguje upozorenja.
     */
    private void validateAudioFeatures(Dataset<Row> data) {
        for (String feature : AUDIO_FEATURES) {
            long outOfRange = data
                    .filter(data.col(feature).isNotNull())
                    .filter(data.col(feature).lt(0.0).or(data.col(feature).gt(1.0)))
                    .count();

            if (outOfRange > 0) {
                logger.warn("UPOZORENJE: {} redova ima {} van opsega [0.0, 1.0]",
                        outOfRange, feature);
            }
        }
    }

    /**
     * Filtrira dataset zadržavajući samo redove sa validnim audio karakteristikama.
     */
    private Dataset<Row> filterValidAudioFeatures(Dataset<Row> data) {
        // Kreira se baza filtera koja inicijalno prihvata sve redove (svaki uslov će se nadovezati sa AND)
        Column filterCondition = lit(true);

        // Dodaj uslov za svaku audio karakteristiku
        for (String feature : AUDIO_FEATURES) {
            filterCondition = filterCondition.and(
                    col(feature).geq(0.0).and(col(feature).leq(1.0))
            );
        }

        // Primjeni filter
        return data.filter(filterCondition);
    }

}