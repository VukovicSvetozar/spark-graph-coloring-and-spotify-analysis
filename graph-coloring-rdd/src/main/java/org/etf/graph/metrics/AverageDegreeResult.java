package org.etf.graph.metrics;

/**
 * Rezultat analize prosječnog stepena grafa (Zadatak 1).
 *   averageDegree - Prosječan stepen svih čvorova u grafu
 *   maxDegree - Maksimalni stepen čvora
 *   executionTimeMs - Vrijeme izvršavanja analize u milisekundama (null ako nije mjereno)
 */
public record AverageDegreeResult(
        double averageDegree,
        int maxDegree,
        Long executionTimeMs
) {
}