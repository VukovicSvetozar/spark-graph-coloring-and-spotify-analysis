package org.etf.graph.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Validacija bojenja grafa koristeći.
 * Pruža dvije glavne funkcionalnosti:
 * - Vraća ukupan broj konflikata u bojenju
 * - Loguje prvih N konflikata (za debugging)
 */
public class GraphValidatorHelper {

    private static final Logger LOG = LogManager.getLogger(GraphValidatorHelper.class);

    /**
     * Računa ukupan broj konflikata u obojenom grafu.
     */
    public static long countConflicts(JavaPairRDD<Integer, Node> nodesRDD) {
        return validateAndGetConflicts(nodesRDD).count();
    }

    /**
     * Validira bojenje grafa i loguje prvih N konflikata.
     * Koristi kada želimo detaljne informacije o konfliktima (npr. koje tačno ivice su problematične).
     */
    public static void validateColoring(JavaPairRDD<Integer, Node> nodesRDD, int sampleConflictsToPrint) {
        JavaRDD<String> conflictDescriptions = validateAndGetConflicts(nodesRDD).persist(StorageLevel.MEMORY_ONLY());

        long totalConflicts = conflictDescriptions.count();

        if (totalConflicts > 0) {
            LOG.warn("Validacija bojenja: Pronađeno {} konflikat(a). Loguje se prvih {}.",
                    totalConflicts, sampleConflictsToPrint);

            List<String> sampleConflicts = conflictDescriptions.take(sampleConflictsToPrint);  // koristi keš
            for (String conflict : sampleConflicts) {
                LOG.warn(conflict);
            }
            if (totalConflicts > sampleConflictsToPrint) {
                LOG.warn("Ima još {} konflikat(a) koji nisu prikazani.",
                        totalConflicts - sampleConflictsToPrint);
            }
        } else {
            LOG.info("Validacija bojenja: Bojenje je validno - nema konflikata.");
        }

        conflictDescriptions.unpersist();
    }


    /**
     * Detektuje konflikte u bojenju grafa i vraća ih kao RDD stringove (opisuje konflikte).
     */
    private static JavaRDD<String> validateAndGetConflicts(JavaPairRDD<Integer, Node> nodesRDD) {

        // Svaki čvor emituje svoje ivice ka susjedima.
        // Koristi se kanonički oblik: (min(A,B), max(A,B)) da se označi ista ivica.
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> edgeEndpoints = nodesRDD
                .flatMapToPair(t -> {
                    int id = t._1();
                    Node node = t._2();
                    int color = node.color();
                    List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> out = new ArrayList<>();

                    for (Integer nb : node.neighbors()) {
                        int minId = Math.min(id, nb);
                        int maxId = Math.max(id, nb);
                        Tuple2<Integer, Integer> edgeKey = new Tuple2<>(minId, maxId);
                        Tuple2<Integer, Integer> endpointInfo = new Tuple2<>(id, color);
                        out.add(new Tuple2<>(edgeKey, endpointInfo));
                    }
                    return out.iterator();
                });

        // Ivice se grupišu - svaka ivica ima informacije o bojama oba kraja.
        JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Integer>>> edgeGroups =
                edgeEndpoints.groupByKey();

        //  Provjera: ako su oba kraja obojeni i imaju istu boju → KONFLIKT.
        return edgeGroups.flatMap(t -> {
            Tuple2<Integer, Integer> edge = t._1();
            List<Tuple2<Integer, Integer>> endpoints = new ArrayList<>();
            t._2().forEach(endpoints::add);

            if (endpoints.size() != 2) {
                LOG.warn("Ivica ({}, {}) ima {} endpoint(a) umjesto 2. " +
                                "Mogući duplikati ivica ili nevalidna struktura grafa.",
                        edge._1(), edge._2(), endpoints.size());
                return Collections.emptyIterator();
            }

            int colorA = endpoints.get(0)._2();
            int colorB = endpoints.get(1)._2();
            int nodeIdA = endpoints.get(0)._1();
            int nodeIdB = endpoints.get(1)._1();

            // Provjera konflikta.
            if (colorA == colorB && colorA != -1) {
                String conflict = String.format(
                        "KONFLIKT: Susjedni čvorovi %d i %d imaju istu boju %d",
                        nodeIdA, nodeIdB, colorA
                );
                return Collections.singletonList(conflict).iterator();
            }

            // Nema konflikta - ivica je validno obojena.
            return Collections.emptyIterator();
        });
    }

}