package org.etf.graph.metrics;

import java.io.Serializable;

/**
 * Rezultat validacije bojenja grafa (Zadatak 3).
 *   isValid - Da li je bojenje validno (nema konflikata)
 *   conflictCount - Broj konfliktnih ivica detektovanih u grafu
 *   executionTimeMs - Vrijeme izvršavanja validacije u milisekundama
 */
public record ValidationResult(
        boolean isValid,
        long conflictCount,
        long executionTimeMs
) implements Serializable {
}