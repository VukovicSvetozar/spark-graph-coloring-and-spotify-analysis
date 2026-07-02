package org.etf.graph.jobs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.etf.graph.config.GraphConfiguration;
import org.etf.graph.core.Node;
import org.etf.graph.core.Timer;
import org.etf.graph.metrics.ColoringResult;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * BASELINE (neoptimizovana) verzija GraphColoringJob za poređenje performansi (Zadatak 5).
 * Karakteristike baseline implementacije:
 * - groupByKey (šalje sve podatke kroz mrežu)
 * - Default particionisanje (ne koristi custom particije)
 * - MEMORY_AND_DISK storage (neserijalizovano keširаnje)
 * Koristi se za benchmark analizu nasuprot optimizovane verzije.
 */
@SuppressWarnings({"DuplicatedCode", "ClassCanBeRecord"})
public class GraphColoringJobBaseline implements GraphColoringExecutor {

    private static final Logger LOG = LogManager.getLogger(GraphColoringJobBaseline.class);

    private final JavaSparkContext jsc;

    public GraphColoringJobBaseline(JavaSparkContext jsc) {
        this.jsc = jsc;
    }

    public ColoringResult execute(List<Node> initialNodes, GraphConfiguration config) {
        LOG.info("=== Pokretanje BASELINE Graph Coloring Job ===");

        try {

            // BASELINE: Default particionisanje (ne koristi config.numPartitions())
            int numPartitions = jsc.defaultParallelism();

            JavaPairRDD<Integer, Node> initialPairRDD = jsc.parallelize(initialNodes, numPartitions)
                    .mapToPair(node -> new Tuple2<>(node.id(), node));

            int maxDegree = config.maxDegree();
            int kStart = (config.initialColors() != null) ? config.initialColors() : (maxDegree + 1);

            JavaPairRDD<Integer, Node> bestResult = null;
            int bestK = -1;

            Timer totalTimer = new Timer();
            totalTimer.start();

            final LongAccumulator messagesSent = jsc.sc().longAccumulator("messagesSent");
            final LongAccumulator candidateFails = jsc.sc().longAccumulator("candidateFails");
            final LongAccumulator tieConflicts = jsc.sc().longAccumulator("tieConflicts");
            final LongAccumulator iterationsAcc = jsc.sc().longAccumulator("iterations");

            for (int k = kStart; k >= 1; k--) {
                LOG.info("(BASELINE) Pokušava se bojenje sa k = {}", k);

                JavaPairRDD<Integer, Node> nodesRDD = initialPairRDD
                        .mapValues(node -> node.withColor(-1))
                        .persist(StorageLevel.MEMORY_AND_DISK());

                Timer kTimer = new Timer();
                kTimer.start();

                messagesSent.reset();
                candidateFails.reset();
                tieConflicts.reset();
                iterationsAcc.reset();

                boolean success = runColoringForK(nodesRDD, k, messagesSent, candidateFails, tieConflicts, iterationsAcc);

                kTimer.stop();

                if (success) {
                    bestK = k;
                    bestResult = nodesRDD;
                    LOG.info("BASELINE USPJEŠNO za k = {} (vrijeme: {} ms)", k, kTimer.getElapsedMillis());
                } else {
                    LOG.warn("BASELINE NEUSPJEŠNO za k = {} (vrijeme: {} ms)", k, kTimer.getElapsedMillis());
                    nodesRDD.unpersist();
                    break;
                }
            }

            totalTimer.stop();

            return new ColoringResult(
                    bestResult,
                    bestK,
                    bestK != -1,
                    totalTimer.getElapsedMillis(),
                    iterationsAcc.value(),
                    messagesSent.value(),
                    candidateFails.value(),
                    tieConflicts.value()
            );
        } catch (Exception e) {
            LOG.error("Greška tokom GraphColoringJobBaseline izvršavanja: {}", e.getMessage(), e);
            throw new RuntimeException("Greška tokom GraphColoringJobBaseline izvršavanja", e);
        }
    }

    private boolean runColoringForK(JavaPairRDD<Integer, Node> nodesRDD,
                                    int k,
                                    LongAccumulator messagesSent,
                                    LongAccumulator candidateFails,
                                    LongAccumulator tieConflicts,
                                    LongAccumulator iterationsAcc) {
        nodesRDD = nodesRDD.persist(StorageLevel.MEMORY_AND_DISK());

        long nodeCount = nodesRDD.count();
        int maxIterations = Math.min((int) (2 * nodeCount), 500);
        boolean aborted = false;
        int stagnation = 0;

        for (int iter = 0; iter < maxIterations; iter++) {
            iterationsAcc.add(1L);

            JavaPairRDD<Integer, Node> uncoloredRDD = nodesRDD
                    .filter(t -> t._2().color() == -1)
                    .persist(StorageLevel.MEMORY_ONLY_SER());

            long uncoloredCount = uncoloredRDD.count();
            if (uncoloredCount == 0) {
                uncoloredRDD.unpersist();
                nodesRDD.unpersist();
                return true;
            }

            // BASELINE: groupByKey (šalje sve boje kroz mrežu, čak i duplikate)
            JavaPairRDD<Integer, Iterable<Integer>> neighborColors = nodesRDD
                    .filter(t -> t._2().color() != -1)
                    .flatMapToPair(t -> {
                        List<Tuple2<Integer, Integer>> out = new ArrayList<>();
                        int color = t._2().color();
                        for (Integer nb : t._2().neighbors()) {
                            out.add(new Tuple2<>(nb, color));
                        }
                        return out.iterator();
                    })
                    .groupByKey();

            JavaPairRDD<Integer, Integer> candidates = uncoloredRDD.leftOuterJoin(neighborColors)
                    .flatMapToPair(t -> {
                        int nodeId = t._1();
                        Optional<Iterable<Integer>> opt = t._2()._2();

                        boolean[] used = new boolean[k];
                        if (opt.isPresent()) {
                            for (Integer c : opt.get()) {
                                if (c != null && c >= 0 && c < k) {
                                    used[c] = true;
                                }
                            }
                        }

                        for (int c = 0; c < k; c++) {
                            if (!used[c]) {
                                return Collections.singletonList(new Tuple2<>(nodeId, c)).iterator();
                            }
                        }
                        return Collections.emptyListIterator();
                    });

            long candidateCount = candidates.count();
            if (candidateCount < uncoloredCount) {
                candidateFails.add(uncoloredCount - candidateCount);
                aborted = true;
                uncoloredRDD.unpersist();
                break;
            }

            JavaPairRDD<Integer, Tuple2<Integer, Integer>> candidateAnnouncements = candidates.join(nodesRDD)
                    .flatMapToPair(t -> {
                        int nodeId = t._1();
                        int cand = t._2()._1();
                        Node node = t._2()._2();
                        List<Tuple2<Integer, Tuple2<Integer, Integer>>> out = new ArrayList<>();
                        for (Integer nb : node.neighbors()) {
                            out.add(new Tuple2<>(nb, new Tuple2<>(cand, nodeId)));
                        }
                        out.add(new Tuple2<>(nodeId, new Tuple2<>(cand, nodeId)));
                        messagesSent.add(out.size());
                        return out.iterator();
                    });

            // BASELINE: groupByKey (više shuffle-a)
            JavaPairRDD<Integer, Integer> decided = candidateAnnouncements
                    .groupByKey()
                    .flatMapToPair(t -> {
                        int nodeId = t._1();
                        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                        t._2().forEach(list::add);

                        Integer myCand = null;
                        for (Tuple2<Integer, Integer> p : list) {
                            if (p._2() == nodeId) {
                                myCand = p._1();
                                break;
                            }
                        }

                        if (myCand == null) return Collections.emptyListIterator();

                        for (Tuple2<Integer, Integer> p : list) {
                            if (p._2() != nodeId && p._1().equals(myCand) && p._2() < nodeId) {
                                tieConflicts.add(1);
                                return Collections.emptyListIterator();
                            }
                        }
                        return Collections.singletonList(new Tuple2<>(nodeId, myCand)).iterator();
                    });

            long decidedCount = decided.count();
            stagnation = (decidedCount == 0) ? stagnation + 1 : 0;
            if (stagnation >= 2) {
                aborted = true;
                uncoloredRDD.unpersist();
                break;
            }

            JavaPairRDD<Integer, Node> updated = nodesRDD.leftOuterJoin(decided)
                    .mapValues(tuple -> tuple._2().isPresent()
                            ? tuple._1().withColor(tuple._2().get())
                            : tuple._1())
                    .persist(StorageLevel.MEMORY_AND_DISK());

            nodesRDD.unpersist();
            nodesRDD = updated;
            if (iter % 5 == 0 && iter > 0) {
                int numPartitions = nodesRDD.getNumPartitions();
                nodesRDD = nodesRDD.repartition(numPartitions)
                        .persist(StorageLevel.MEMORY_AND_DISK());
            }
            uncoloredRDD.unpersist();
        }

        return !aborted && nodesRDD.filter(t -> t._2().color() == -1).count() == 0;

    }

}