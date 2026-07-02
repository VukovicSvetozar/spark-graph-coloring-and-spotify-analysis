package org.etf.spotify.analysis.analyzer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Interfejs za sve analizatore podataka.
 */
public interface Analyzer {
    /**
     * Izvršava analizu nad primljenim setom podataka.
     */
    void analyze(Dataset<Row> data);
}