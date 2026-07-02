package org.etf.spotify.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.etf.spotify.analysis.analyzer.*;
import org.etf.spotify.analysis.config.SparkConfig;
import org.etf.spotify.analysis.exporter.ResultExporter;
import org.etf.spotify.analysis.loader.DataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ulazna tačka aplikacije.
 * Orkestrira izvršavanje svih traženih analiza nad Spotify skupom podataka.
 */
public class SpotifyAnalysisApp {

    private static final Logger logger = LoggerFactory.getLogger(SpotifyAnalysisApp.class);

    private static final String INDENT = "  ";
    private static final String SUCCESS = "✓";
    private static final String ERROR = "✗";
    private static final int TOTAL_ANALYSES = 10; // 1 učitavanje + 9 analiza

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Greška pri pokretanju SpotifyAnalysisApp: Nije navedena ulazna putanja CSV skupa podataka.");
            System.exit(1);
        }

        String inputPath = args[0];
        long startTime = System.currentTimeMillis();

        printHeader();

        SparkSession spark = null;

        try {
            // Inicijalizacija Spark okruženja
            spark = SparkConfig.createSparkSession();
            ResultExporter.initializeOutputDirectory();

            // Učitavanje podataka
            printProgress(1, "Učitavanje podataka");
            DataLoader loader = new DataLoader(spark);
            Dataset<Row> loadedData = loader.loadData(inputPath);

            // Caching za optimizaciju
            final Dataset<Row> spotifyData = loadedData.persist(StorageLevel.MEMORY_AND_DISK());

            long totalRows = spotifyData.count();
            printSuccess("Učitano redova: " + String.format("%,d", totalRows));
            printSuccess("Dataset keširan u memoriju\n");

            // Izvršavanje analiza

            executeAnalysis(2, "Analiza distribucije podataka",
                    "01_distribution_analysis.json",
                    () -> new DataDistributionAnalyzer().analyze(spotifyData));

            executeAnalysis(3, "Analiza kolaboracija",
                    "02_collaboration_analysis.json",
                    () -> new CollaborationAnalyzer().analyze(spotifyData));

            executeAnalysis(4, "Breakthrough pjesme analiza",
                    "03_breakthrough_songs.json",
                    () -> new BreakthroughSongsAnalyzer().analyze(spotifyData));

            executeAnalysis(5, "Sweet spot tempo analiza",
                    "04_tempo_sweetspot.json",
                    () -> new TempoSweetSpotAnalyzer().analyze(spotifyData));

            executeAnalysis(6, "Eksplicitni sadržaj korelacija",
                    "05_explicit_content_correlation.json",
                    () -> new ExplicitContentAnalyzer().analyze(spotifyData));

            executeAnalysis(7, "Duge plesne pjesme analiza",
                    "06_long_danceable_tracks.json",
                    () -> new LongDanceableTracksAnalyzer().analyze(spotifyData));

            executeAnalysis(8, "Eksplicitnost vs valencija",
                    "07_explicitness_valence.json",
                    () -> new ExplicitnessValenceAnalyzer().analyze(spotifyData));

            executeAnalysis(9, "Dosljednost umjetnika",
                    "08_artist_consistency.json",
                    () -> new ArtistConsistencyAnalyzer().analyze(spotifyData));

            executeAnalysis(10, "Akustične vs vokalne pjesme",
                    "09_acoustic_vs_vocal.json",
                    () -> new AcousticInstrumentalAnalyzer().analyze(spotifyData));

            // Oslobađanje keširane memorije
            spotifyData.unpersist();

            // Generisanje finalnog izvještaja
            long totalTime = System.currentTimeMillis() - startTime;
            ResultExporter.setExecutionStats(totalRows, totalTime);
            ResultExporter.createSummaryReport();

            printFooter(totalTime);

        } catch (Exception e) {
            logger.error("Kritična greška tokom izvršavanja analize", e);
            System.err.println("\n" + ERROR + " KRITIčNA GREšKA: " + e.getMessage());
            System.exit(1);
        } finally {
            if (spark != null) {
                logger.info("Zatvaranje Spark sesije...");
                spark.stop();
            }
        }
    }

    /**
     * Izvršava pojedinačnu analizu, tj. zajednički tok izvrsavanja za sve analize.
     * Specifična logika se predaje kroz Runnable parametar.
     */
    private static void executeAnalysis(int current, String analysisName,
                                        String outputFile, Runnable analysis) {
        printProgress(current, analysisName);

        try {
            long startTime = System.currentTimeMillis();
            analysis.run();
            long duration = System.currentTimeMillis() - startTime;

            // Bilježi vrijeme za finalni izvještaj
            ResultExporter.recordAnalysisTiming(analysisName, duration);

            printSuccess(String.format("Rezultat: %s (%.2fs)\n", outputFile, duration / 1000.0));

        } catch (Exception e) {
            logger.error("Greška u analizi: {}", analysisName, e);
            System.err.println(INDENT + ERROR + " GREŠKA u " + outputFile + ": " + e.getMessage());
            throw new RuntimeException("Analiza nije bila uspješna: " + analysisName, e);
        }
    }

    /**
     * Ispis zaglavlja aplikacije
     */
    private static void printHeader() {
        System.out.println("-------------------------------------------------------------------");
        System.out.println(INDENT + INDENT + INDENT + "SPOTIFY DATA ANALYSIS - Analiza Spotify Skupa Podataka");
        System.out.println("-------------------------------------------------------------------");
    }

    /**
     * Ispis progress bar-a sa trenutnim korakom izvršavanja
     */
    private static void printProgress(int current, String message) {
        System.out.printf("[%d/%d] %s...%n", current, TOTAL_ANALYSES, message);
    }

    /**
     * Ispis poruke o uspjehu
     */
    private static void printSuccess(String message) {
        System.out.println(INDENT + SUCCESS + " " + message);
    }

    /**
     * Ispis footer-a sa završnim rezultatima izvršavanja
     */
    private static void printFooter(long totalTime) {
        System.out.println("-------------------------------------------------------------------");
        System.out.println(INDENT + INDENT + INDENT + "SVE ANALIZE SU USPJEŠNO ZAVRŠENE! " + SUCCESS);
        System.out.println("-------------------------------------------------------------------");
        System.out.printf("Generisani fajlovi:%n");
        System.out.println(INDENT + "- 9 x JSON fajlova (results/01_*.json - results/09_*.json)");
        System.out.println(INDENT + "- 1 x Summary report (results/SUMMARY_REPORT.md)");
        System.out.printf("%nLokacija: ./results/%n");
        System.out.printf("Ukupno vrijeme: %.2f sekundi%n", totalTime / 1000.0);
        System.out.println("-------------------------------------------------------------------\n");
    }

}