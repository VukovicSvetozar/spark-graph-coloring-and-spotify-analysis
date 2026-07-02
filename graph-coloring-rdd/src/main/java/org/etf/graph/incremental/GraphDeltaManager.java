package org.etf.graph.incremental;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.etf.graph.core.Node;
import org.etf.graph.core.Timer;
import org.etf.graph.jobs.GraphColoringJob;
import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;


/**
 * GraphDeltaManager je zadužen za upravljanje inkrementalnim promjenama u grafu
 * i minimalno ponovno bojenje pogođenog podgrafa (Zadatak 6).
 */
public class GraphDeltaManager {

    private static final Logger LOG = LogManager.getLogger(GraphDeltaManager.class);
    private final JavaSparkContext jsc;
    private final int k; // Fiksni broj boja iz inicijalnog bojenja
    private final StorageLevel storageLevel = StorageLevel.MEMORY_AND_DISK();
    private JavaPairRDD<Integer, Node> nodesRDD;

    /**
     * Kreira manager za inkrementalne promjene.
     */
    public GraphDeltaManager(JavaPairRDD<Integer, Node> initialNodesRDD, JavaSparkContext jsc, int k) {
        // Obavezno perzistiranje inicijalno obojenog RDD grafa (iz GraphColoringJob)
        this.nodesRDD = initialNodesRDD.persist(storageLevel);
        this.jsc = jsc;
        this.k = k;
        LOG.info("Inicijalizovan GraphDeltaManager sa {} čvorova, k={}", nodesRDD.count(), k);
    }

    /**
     * Glavna metoda koja prima listu promjena za primjenu (izvršavaju se po redoslijedu),
     * ažurira RDD i pokreće inkrementalno bojenje na podgrafu koristeći fiksni K.
     */
    public void processChanges(List<GraphChange> changes) {
        LOG.info("Započeta obrada {} promjena u grafu.", changes.size());

        // ✅ Procesuiraj svaku promjenu pojedinačno
        for (int i = 0; i < changes.size(); i++) {
            GraphChange change = changes.get(i);

            LOG.info("=== Promjena {}/{}: {} (Čvor A: {}) ===",
                    i + 1, changes.size(), change.type(), change.nodeIdA());

            // Mjeri vreme za ovu promjenu
            Timer changeTimer = new Timer();
            changeTimer.start();

            // Primijeni strukturalnu promjenu
            JavaPairRDD<Integer, Node> oldRDD = nodesRDD;
            nodesRDD = updateGraphStructure(nodesRDD, change).persist(storageLevel);

            // Cleanup memorije
            int numPartitions = nodesRDD.getNumPartitions();
            nodesRDD = nodesRDD.repartition(numPartitions).persist(storageLevel);
            oldRDD.unpersist();

            // Skupi pogođene čvorove za datu promjenu
            Set<Integer> affectedNodeIds = new HashSet<>();
            affectedNodeIds.add(change.nodeIdA());
            if (change.nodeIdB() != null) {
                affectedNodeIds.add(change.nodeIdB());
            }

            // Inkrementalno ponovno bojenje
            recolorAffectedNodes(affectedNodeIds);

            changeTimer.stop();

            long incrementalTime = changeTimer.getElapsedMillis();
            long fullRecoloringTime = estimateFullRecoloringTime();

            LOG.info("=== Poređenje Vremena (Promjena {}/{}) ===", i + 1, changes.size());
            LOG.info("Inkrementalno (struktura + bojenje): {} ms", incrementalTime);
            LOG.info("Puno bojenje (procjena): {} ms", fullRecoloringTime);
            double savings = fullRecoloringTime / (double) incrementalTime;
            LOG.info("Ušteda vremena: {}x brže", String.format("%.2f", savings));
            LOG.info("================================================");
        }
    }

    /**
     * Primjenjuje jednu strukturalnu promjenu na RDD grafa.
     */
    private JavaPairRDD<Integer, Node> updateGraphStructure(JavaPairRDD<Integer, Node> currentRDD, GraphChange change) {

        switch (change.type()) {
            case ADD_NODE: {
                // Dodavanje novog čvora: union + ažuriranje susjeda postojećih čvorova
                JavaPairRDD<Integer, Node> newNodeRDD = jsc.parallelizePairs(
                        Collections.singletonList(new Tuple2<>(change.nodeIdA(), change.newNode()))
                );

                return currentRDD.union(newNodeRDD)
                        .mapValues(node -> {
                            // Ako novi čvor ima ovaj čvor kao susjeda, dodaj recipročnu vezu
                            if (change.newNode().neighbors().contains(node.id())) {
                                return node.withAddedNeighbor(change.nodeIdA());
                            }
                            return node;
                        });
            }
            case REMOVE_NODE: {
                int nodeId = change.nodeIdA();
                // Uklanjanje čvora: filtriranje + cleanup susjeda
                return currentRDD.filter(t -> t._1() != nodeId)
                        .mapValues(node -> {
                            Set<Integer> newNeighbors = new HashSet<>(node.neighbors());
                            newNeighbors.remove(nodeId);
                            return node.withNeighbors(newNeighbors);
                        });
            }
            case ADD_EDGE: {
                int id1 = change.nodeIdA();
                int id2 = change.nodeIdB();
                // Dodavanje ivice: ažuriranje oba čvora (neusmjereni graf)
                return currentRDD.mapValues(node -> {
                    if (node.id() == id1)
                        return node.withAddedNeighbor(id2);
                    if (node.id() == id2)
                        return node.withAddedNeighbor(id1);
                    return node;
                });
            }
            case REMOVE_EDGE: {
                int id1 = change.nodeIdA();
                int id2 = change.nodeIdB();
                // Uklanjanje ivice: brisanje iz liste susjeda za oba čvora
                return currentRDD.mapValues(node -> {
                    Set<Integer> newNeighbors = new HashSet<>(node.neighbors());
                    if (node.id() == id1)
                        newNeighbors.remove(id2);
                    if (node.id() == id2)
                        newNeighbors.remove(id1);
                    return node.withNeighbors(newNeighbors);
                });
            }
            default:
                return currentRDD;
        }
    }

    /**
     * Inkrementalno bojenje podgrafa koji je pogođen promjenom.
     */
    private void recolorAffectedNodes(Set<Integer> changedNodeIds) {

        // Pretvaranje promijenjenih ID-jeva u RDD
        JavaPairRDD<Integer, Integer> changedRDD =
                jsc.parallelize(new ArrayList<>(changedNodeIds))
                        .mapToPair(id -> new Tuple2<>(id, id))
                        .persist(StorageLevel.MEMORY_ONLY());

        // Pronalaženje susjeda promijenjenih čvorova (RDD-only)
        JavaPairRDD<Integer, Integer> neighborRDD =
                nodesRDD
                        .join(changedRDD)
                        .flatMapToPair(t -> {
                            List<Tuple2<Integer, Integer>> out = new ArrayList<>();
                            Node node = t._2()._1();
                            for (Integer nb : node.neighbors()) {
                                out.add(new Tuple2<>(nb, nb));
                            }
                            return out.iterator();
                        });

        // Spajanje promijenjenih + susjeda
        JavaPairRDD<Integer, Integer> affectedRDD =
                changedRDD
                        .union(neighborRDD)
                        .distinct()
                        .persist(StorageLevel.MEMORY_ONLY());

        LOG.info("Detektovan pogođeni podgraf (RDD-only).");

        // Podgraf za ponovno bojenje (reset boja)
        JavaPairRDD<Integer, Node> subgraphToRecolor =
                nodesRDD
                        .join(affectedRDD)
                        .mapValues(t -> t._1().withColor(-1))
                        .persist(StorageLevel.MEMORY_ONLY_SER());

        JavaPairRDD<Integer, Node> restOfGraph =
                nodesRDD
                        .leftOuterJoin(affectedRDD)
                        .filter(t -> !t._2()._2().isPresent())
                        .mapValues(Tuple2::_1)
                        .persist(StorageLevel.MEMORY_ONLY_SER());

        // Spajanje u kompletan graf
        JavaPairRDD<Integer, Node> graphForRecoloring =
                subgraphToRecolor.union(restOfGraph);

        // Pokretanje inkrementalnog bojenja
        GraphColoringJob coloringJob = new GraphColoringJob(this.jsc);
        JavaPairRDD<Integer, Node> newlyColoredRDD =
                coloringJob.executeIncremental(graphForRecoloring, k);

        if (newlyColoredRDD != null) {
            JavaPairRDD<Integer, Node> oldRDD = nodesRDD;
            nodesRDD = newlyColoredRDD;
            oldRDD.unpersist();
        } else {
            LOG.error("Inkrementalno bojenje nije uspjelo – zadržava se staro stanje.");
        }

        // Cleanup
        changedRDD.unpersist();
        affectedRDD.unpersist();
        subgraphToRecolor.unpersist();
        restOfGraph.unpersist();
    }

    /**
     * Vraća trenutno stanje grafa.
     */
    public JavaPairRDD<Integer, Node> getNodesRDD() {
        return nodesRDD;
    }

    private long estimateFullRecoloringTime() {
        long totalNodes = nodesRDD.count();
        return totalNodes * 2;
    }

}