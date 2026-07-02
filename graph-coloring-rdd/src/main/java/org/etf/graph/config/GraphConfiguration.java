package org.etf.graph.config;

import java.io.Serializable;

/**
 * Klasa za čuvanje konfiguracionih parametara parsiranih iz CLI-a.
 */
public record GraphConfiguration(
        String inputPath,
        Integer numNodes,
        Integer maxDegree,
        Long seed,
        Integer initialColors,
        Integer numPartitions,
        boolean runAverageCheck,
        boolean runColoring,
        boolean runValidation,
        boolean runIncremental,
        boolean measureTime,
        String metricsOutPath,
        String outputGraphPath,
        String outputResultPath,
        boolean useBaseline,
        String changesFilePath
) implements Serializable {
}
