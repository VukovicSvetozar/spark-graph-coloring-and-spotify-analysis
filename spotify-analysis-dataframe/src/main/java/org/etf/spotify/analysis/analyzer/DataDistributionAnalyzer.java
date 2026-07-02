package org.etf.spotify.analysis.analyzer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.etf.spotify.analysis.exporter.ResultExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * Analizator za distribuciju podataka u Spotify datasetu.
 * Za svaku kolonu izvodi specifičnu analizu prema tipu podataka:
 * - Numeričke (int, long, float, double): mean, stddev, min, max, percentili
 * - String: top 10 najčešćih vrednosti
 * - Boolean: true/false distribucija sa procentima
 * Izvozi kompletne rezultate u JSON format.
 */
public class DataDistributionAnalyzer implements Analyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataDistributionAnalyzer.class);

    private final Map<String, Map<String, Object>> distributionResults;

    public DataDistributionAnalyzer() {
        this.distributionResults = new HashMap<>();
    }

    /**
     * Glavna metoda analize - iterira kroz sve kolone i poziva analizu za svaku.
     */
    @Override
    public void analyze(Dataset<Row> data) {
        String[] columns = data.columns();

        LOGGER.info("Pokretanje analize distribucije za {} kolona", columns.length);

        // Analiza svake pojedinačne kolone
        for (String column : columns) {
            analyzeColumn(data, column);
        }

        LOGGER.info("Analiza distribucije završena. Izvoz rezultata...");

        // Kreiranje strukture rezultata i izvoz u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Analiza Distribucije Podataka",
                "Statistička analiza distribucije svih kolona u datasetu"
        );
        result.put("total_rows", data.count());
        result.put("total_columns", columns.length);
        result.put("column_distributions", distributionResults);

        ResultExporter.exportAnalysis(
                "01_distribution_analysis.json",
                result,
                "Analiza Distribucije Podataka\n" +
                        "Ukupno kolona: " + columns.length + "\n" +
                        "Detalji: `01_distribution_analysis.json`"
        );
    }

    /**
     * Analizira distribuciju pojedinačne kolone prema tipu podataka.
     */
    private void analyzeColumn(Dataset<Row> data, String column) {
        Map<String, Object> stats = new HashMap<>();

        try {
            // Detekcija tipa podataka
            String dataType = data.schema().apply(column).dataType().typeName();
            stats.put("data_type", dataType);

            // Ukupan broj redova
            long count = data.select(column).count();
            stats.put("count", count);

            // Broj null vrijednosti
            long nullCount = data.filter(data.col(column).isNull()).count();
            stats.put("null_count", nullCount);
            stats.put("non_null_count", count - nullCount);

            // Broj jedinstvenih vrijednosti
            long distinctCount = data.select(column).distinct().count();
            stats.put("distinct_count", distinctCount);

            // Dodatna statistika u zavisnosti od tipa
            if (isNumericType(dataType)) {
                analyzeNumericColumn(data, column, stats);
            } else if (dataType.equals("string")) {
                analyzeStringColumn(data, column, stats);
            } else if (dataType.equals("boolean")) {
                analyzeBooleanColumn(data, column, stats);
            }

            // Sačuvaj rezultate
            distributionResults.put(column, stats);

            LOGGER.info("Kolona [{}] -> Tip: {} | Total: {} | Unique: {}",
                    column, dataType, count, distinctCount);

        } catch (Exception e) {
            LOGGER.error("Greška pri analizi kolone: {}", column, e);
        }
    }

    /**
     * Analizira numeričku kolonu.
     * Metrike:
     * - mean (projek)
     * - stddev (standardna devijacija)
     * - min (minimum)
     * - max (maksimum)
     * - 25%, 50%, 75% percentili (kvartili)
     */
    private void analyzeNumericColumn(Dataset<Row> data, String column, Map<String, Object> stats) {
        try {
            // Osnovne statistike (mean, stddev, min, max)
            Row basicStats = data
                    .select(column)
                    .filter(col(column).isNotNull())
                    .agg(
                            mean(col(column)).alias("mean"),
                            stddev(col(column)).alias("stddev"),
                            min(col(column)).alias("min"),
                            max(col(column)).alias("max")
                    )
                    .first();

            stats.put("mean", basicStats.getAs("mean"));
            stats.put("stddev", basicStats.getAs("stddev"));
            stats.put("min", basicStats.getAs("min"));
            stats.put("max", basicStats.getAs("max"));

            // Percentili
            Row percentiles = data
                    .select(column)
                    .filter(col(column).isNotNull())
                    .selectExpr(
                            "percentile_approx(" + column + ", 0.25) as p25",
                            "percentile_approx(" + column + ", 0.50) as p50",
                            "percentile_approx(" + column + ", 0.75) as p75"
                    )
                    .first();

            stats.put("25%", percentiles.get(0));
            stats.put("50%", percentiles.get(1));
            stats.put("75%", percentiles.get(2));

        } catch (Exception e) {
            LOGGER.warn("Greška pri analizi numeričke kolone {}: {}", column, e.getMessage());
            // Fallback na describe() ako percentile_approx ne radi
            LOGGER.warn("Greška pri analizi numeričke kolone {}: {}. Koristi se fallback (.describe() metoda)",
                    column, e.getMessage());
            Dataset<Row> numStats = data.select(column).describe();
            for (Row row : numStats.collectAsList()) {
                String metric = row.getString(0);
                String value = row.getString(1);
                if (value != null && !metric.equals("count")) {
                    try {
                        stats.put(metric, Double.parseDouble(value));
                    } catch (NumberFormatException ex) {
                        stats.put(metric, value);
                    }
                }
            }
        }
    }

    /**
     * Metoda za nalizu string kolona.
     * Metrika:
     * - Top 10 najčešćih vrijednosti sa brojem pojavljivanja
     */
    private void analyzeStringColumn(Dataset<Row> data, String column, Map<String, Object> stats) {
        Dataset<Row> topValues = data.groupBy(column)
                .count()
                .orderBy(col("count").desc())
                .limit(10);

        // Konverzija u mapu za JSON izvoz
        Map<String, Long> top10 = new HashMap<>();
        for (Row row : topValues.collectAsList()) {
            String value = row.getString(0);
            if (value != null) {
                top10.put(value, row.getLong(1));
            }
        }
        stats.put("top_10_values", top10);
    }

    /**
     * Metoda za analizu boolean kolona.
     * Metrike:
     * - true_count (broj true vrijednosti)
     * - false_count (broj false vrijednosti)
     * - true_percentage (procenat true vrijednosti)
     * - false_percentage (procenat false vrijednosti)
     */
    private void analyzeBooleanColumn(Dataset<Row> data, String column, Map<String, Object> stats) {
        long trueCount = data.filter(data.col(column).equalTo(true)).count();
        long falseCount = data.filter(data.col(column).equalTo(false)).count();

        stats.put("true_count", trueCount);
        stats.put("false_count", falseCount);

        long total = trueCount + falseCount;
        if (total > 0) {
            stats.put("true_percentage", (trueCount * 100.0) / total);
            stats.put("false_percentage", (falseCount * 100.0) / total);
        }
    }

    /**
     * Provjera da li je tip numerički
     */
    private boolean isNumericType(String dataType) {
        return dataType.equals("integer") || dataType.equals("double") ||
                dataType.equals("long") || dataType.equals("float");
    }

}