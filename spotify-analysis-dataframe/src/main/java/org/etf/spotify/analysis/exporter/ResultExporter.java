package org.etf.spotify.analysis.exporter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Univerzalni exporter za sve analize
 * Kreira strukturirane JSON fajlove i Markdown summary
 */
public class ResultExporter {
    private static final Logger logger = LoggerFactory.getLogger(ResultExporter.class);
    private static final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .serializeSpecialFloatingPointValues()
            .create();

    private static final String OUTPUT_DIR = "results";

    private static final List<String> summaryLines = new CopyOnWriteArrayList<>();
    private static final Map<String, Long> analysisTimings = new LinkedHashMap<>();
    private static long totalRowsProcessed = 0;
    private static long totalExecutionTime = 0;

    private ResultExporter() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static synchronized void initializeOutputDirectory() {
        try {
            File dir = new File(OUTPUT_DIR);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                if (created) {
                    logger.info("Izlazni direktorijum kreiran: {}", OUTPUT_DIR);
                } else {
                    logger.error("Nije moguće kreirati direktorijum: {}", OUTPUT_DIR);
                }
            } else {
                logger.info("Izlazni direktorijum već postoji: {}", OUTPUT_DIR);
            }

            // Očisti stare podatke
            summaryLines.clear();
            analysisTimings.clear();
            totalRowsProcessed = 0;
            totalExecutionTime = 0;

        } catch (Exception e) {
            logger.error("Greška pri kreiranju izlaznog direktorijuma", e);
        }
    }

    /**
     * Postavlja statistiku izvršavanja (ukupan broj redova i ukupno vrijeme izvršavanja)
     */
    public static synchronized void setExecutionStats(long rows, long totalTime) {
        totalRowsProcessed = rows;
        totalExecutionTime = totalTime;
    }

    /**
     * Bilježi vrijeme izvršavanja pojedinacne analize u ms
     */
    public static synchronized void recordAnalysisTiming(String analysisName, long duration) {
        analysisTimings.put(analysisName, duration);
    }

    /**
     * Izvozi analizu u JSON fajl
     */
    public static synchronized void exportAnalysis(String filename, Object data, String summaryText) {
        // Osiguraj da direktorijum postoji
        File dir = new File(OUTPUT_DIR);
        if (!dir.exists()) {
            boolean success = dir.mkdirs();
            if (!success) {
                logger.error("Neuspješno kreiranje direktorijuma: {}", OUTPUT_DIR);
                throw new RuntimeException("Nije moguće kreirati direktorijum za izvoz: " + OUTPUT_DIR);
            }
        }

        String filepath = OUTPUT_DIR + File.separator + filename;

        try (FileWriter writer = new FileWriter(filepath)) {
            gson.toJson(data, writer);

            if (summaryText != null) {
                summaryLines.add(summaryText);
            }
        } catch (IOException e) {
            logger.error("Greška pri izvozu: {}", filename, e);
            throw new RuntimeException("Greška pri izvozu: " + filename, e);
        }
    }

    /**
     * Kreira finalni izvještaj (summary report) sa svim rezultatima i statistikom izvršavanja
     */
    public static synchronized void createSummaryReport() {
        File dir = new File(OUTPUT_DIR);
        if (!dir.exists()) {
            boolean success = dir.mkdirs();
            if (!success) {
                logger.error("Kreiranje direktorijuma nije bilo uspješno: {}", OUTPUT_DIR);
                throw new RuntimeException("Nije moguće kreirati direktorijum za završni izvještaj: " + OUTPUT_DIR);
            }
        }

        String filepath = OUTPUT_DIR + File.separator + "SUMMARY_REPORT.md";

        try (FileWriter writer = new FileWriter(filepath)) {
            // Zaglavlje
            writer.write("# SPOTIFY DATA ANALYSIS - Finalni Izvještaj\n\n");
            writer.write("**Datum:** " + new Date() + "\n");
            writer.write("**Projekat:** Odabrana Poglavlja Operativnih Sistema\n");
            writer.write("**Framework:** Apache Spark DataFrame API (3.5.0)\n");
            writer.write("**Jezik:** Java 11+\n\n");
            writer.write("---\n\n");

            // Rezime izvršavanja
            writer.write("## Rezime Izvršavanja\n\n");
            writer.write(String.format("| %-35s | %-20s |\n", "Metrika", "Vrednost"));
            writer.write(String.format("|-%-35s-|-%-20s-|\n", "-----------------------------------", "--------------------"));
            writer.write(String.format("| %-35s | %-20s |\n", "**Dataset veličina**", String.format("%,d redova", totalRowsProcessed)));
            writer.write(String.format("| %-35s | %-20s |\n", "**Ukupno vrijeme**", String.format("%.2f sekundi", totalExecutionTime / 1000.0)));
            writer.write(String.format("| %-35s | %-20s |\n", "**Prosječno vrijeme po analizi**", String.format("%.2f sekundi", totalExecutionTime / 9.0 / 1000.0)));
            writer.write(String.format("| %-35s | %-20s |\n", "**Broj analiza**", "9 (1 + 8 dodatnih)"));
            writer.write(String.format("| %-35s | %-20s |\n", "**Ukupno bodova**", "20 (4 + 8×2)"));
            writer.write("\n");

            // Pojedinačna vremena
            if (!analysisTimings.isEmpty()) {
                writer.write("### Pojedinačna Vremena Izvršavanja\n\n");

                String rowFormat = "| %-3s | %-35s | %-12s | %-10s |\n";
                writer.write(String.format(rowFormat, "#", "Analiza", "Vrijeme", "% Ukupnog"));
                writer.write("|-----|-------------------------------------|--------------|------------|\n");

                int i = 1;
                for (Map.Entry<String, Long> entry : analysisTimings.entrySet()) {
                    double seconds = entry.getValue() / 1000.0;
                    double percentage = (entry.getValue() * 100.0) / totalExecutionTime;

                    writer.write(String.format(rowFormat,
                            i++,
                            entry.getKey(),
                            String.format("%.2f s", seconds),
                            String.format("%.1f%%", percentage)
                    ));
                }
                writer.write("\n");

                long sumTimings = analysisTimings.values().stream()
                        .mapToLong(Long::longValue)
                        .sum();
                writer.write("**Napomena:** Ukupno vrijeme analiza: " +
                        String.format("%.2f s", sumTimings / 1000.0) + "\n\n");
            }

            writer.write("---\n\n");

            // Pregled analiza
            writer.write("## Pregled Analiza\n\n");

            int taskNum = 1;
            for (String summary : summaryLines) {
                // ✅ BOLJE FORMATIRANJE - izvuci naslov iz prve linije
                String[] lines = summary.split("\n");
                String title = lines[0].trim();

                writer.write("### " + taskNum + ". " + title + "\n\n");

                // Ispiši ostale linije (detalje)
                writer.write("```\n");
                for (int j = 1; j < lines.length; j++) {
                    writer.write(lines[j] + "\n");
                }
                writer.write("```\n\n");

                taskNum++;
            }

            writer.write("---\n\n");

            // Zaključak
            writer.write("## Zaključak\n\n");
            writer.write("Sve analize uspješno izvršene koristeći **Apache Spark DataFrame API**.\n");
            writer.write("Rezultati su dostupni u odvojenim JSON fajlovima za svaki zadatak.\n\n");

            writer.write("**Generisani fajlovi:**\n\n");
            writer.write("- **9 × JSON** fajlova (jedan po analizi)\n");
            writer.write("- **1 × SUMMARY_REPORT.md** (ovaj fajl)\n\n");

            writer.write("---\n\n");

            logger.info("✓ Finalni izvještaj kreiran: SUMMARY_REPORT.md");

        } catch (IOException e) {
            logger.error("Greška pri kreiranju finalnog izvještaja", e);
            throw new RuntimeException("Greška pri kreiranju finalnog izvještaja", e);
        }
    }

    /**
     * Pomoćna metoda za kreiranje strukture rezultata
     */
    public static Map<String, Object> createResult(String analysisName, String description) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("analysis_name", analysisName);
        result.put("description", description);
        result.put("timestamp", new Date().toString());
        return result;
    }

}