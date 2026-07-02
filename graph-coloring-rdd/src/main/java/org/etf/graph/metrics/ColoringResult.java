package org.etf.graph.metrics;

import org.apache.spark.api.java.JavaPairRDD;
import org.etf.graph.core.Node;

/**
 * Rezultat distribuiranog bojenja grafa (Zadatak 2).
 *   coloredRDD - RDD obojenih čvorova
 *   bestK - Minimalni broj boja pronađen iterativnim smanjenjem (hromatski broj)
 *   success - Da li je bojenje uspješno završeno
 *   elapsedMs - Ukupno vrijeme izvršavanja svih K pokušaja u milisekundama
 *   iterations - Ukupan broj iteracija algoritma kroz sve K pokušaje
 *   messagesSent - Ukupan broj poruka razmijenjenih između čvorova (shuffle metrika)
 *   candidateFails - Broj slučajeva gdje čvor nije mogao naći kandidatsku boju
 *   tieConflicts - Broj situacija gdje dva susjedna čvora biraju istu
 */
public record ColoringResult(
        JavaPairRDD<Integer, Node> coloredRDD,
        int bestK,
        boolean success,
        long elapsedMs,
        long iterations,
        long messagesSent,
        long candidateFails,
        long tieConflicts
) {
}