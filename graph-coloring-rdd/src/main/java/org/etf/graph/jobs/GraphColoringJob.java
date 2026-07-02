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

import java.util.*;

/**
 * Implementacija distribuiranog algoritma za bojenje grafa (Zadatak 2).
 * Optimizacije (Zadatak 5):
 * - aggregateByKey umjesto groupByKey -> map-side combine, redukcija shuffle operacija
 * - MEMORY_ONLY_SER za privremene RDD-ove -> serijalizovano keširanje, manja memorija
 * - Custom particionisanje -> bolja distribucija posla
 */
@SuppressWarnings({"DuplicatedCode", "ClassCanBeRecord"})
public class GraphColoringJob implements GraphColoringExecutor {

    private static final Logger LOG = LogManager.getLogger(GraphColoringJob.class);

    private final JavaSparkContext jsc;

    public GraphColoringJob(JavaSparkContext jsc) {
        this.jsc = jsc;
    }

    /**
     * Glavna metoda za bojenje grafa sa iterativnim smanjenjem broja boja (K).
     * Počinje sa K = maxDegree + 1 i smanjuje ga dok ne pronađe minimum.
     */
    public ColoringResult execute(List<Node> initialNodes, GraphConfiguration config) {
        LOG.info("=== Pokretanje GraphColoringJob ===");

        try {

            // Inicijalizacija (optimizacija: određivanje broja particija (custom ili default))
            int numPartitions = (config.numPartitions() != null)
                    ? config.numPartitions()
                    : jsc.defaultParallelism();

            JavaPairRDD<Integer, Node> initialPairRDD = jsc.parallelize(initialNodes, numPartitions)
                    .mapToPair(node -> new Tuple2<>(node.id(), node));

            // Određivanje početnog broja boja (K)
            int maxDegree = config.maxDegree();
            int kStart = (config.initialColors() != null) ? config.initialColors() : (maxDegree + 1);

            JavaPairRDD<Integer, Node> bestResult = null;
            int bestK = -1;

            // Pokretanje tajmera za ukupno vrijeme izvršavanja
            Timer totalTimer = new Timer();
            totalTimer.start();

            // Kreiranje akumulatora za praćenje metrika
            final LongAccumulator messagesSent = jsc.sc().longAccumulator("messagesSent");
            final LongAccumulator candidateFails = jsc.sc().longAccumulator("candidateFails");
            final LongAccumulator tieConflicts = jsc.sc().longAccumulator("tieConflicts");
            final LongAccumulator iterationsAcc = jsc.sc().longAccumulator("iterations");

            // Iterativno smanjivanje K dok se ne pronađe minimalan broj boja
            for (int k = kStart; k >= 1; k--) {
                LOG.info("Pokušava se bojenje sa k = {}", k);

                // Resetovanje svih čvorova na neobojeno stanje (-1)
                JavaPairRDD<Integer, Node> nodesRDD = initialPairRDD
                        .mapValues(node -> node.withColor(-1))
                        .persist(StorageLevel.MEMORY_AND_DISK());

                Timer kTimer = new Timer();
                kTimer.start();

                messagesSent.reset();
                candidateFails.reset();
                tieConflicts.reset();
                iterationsAcc.reset();

                // Pokušaj bojenja sa trenutnim K
                boolean success = runColoringForK(nodesRDD, k, messagesSent, candidateFails, tieConflicts, iterationsAcc);

                kTimer.stop();

                if (success) {
                    bestK = k;
                    bestResult = nodesRDD;
                    LOG.info("USPJEŠNO za k = {} (vrijeme: {} ms)", k, kTimer.getElapsedMillis());
                } else {
                    LOG.warn("NEUSPJEŠNO za k = {} (vrijeme: {} ms)", k, kTimer.getElapsedMillis());
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
            LOG.error("Greška tokom GraphColoringJob izvršavanja: {}", e.getMessage(), e);
            throw new RuntimeException("Greška tokom GraphColoringJob izvršavanja", e);
        }
    }

    /**
     * Implementacija algoritma bojenja za fiksni broj boja K.
     */
    private boolean runColoringForK(JavaPairRDD<Integer, Node> nodesRDD,
                                    int k,
                                    LongAccumulator messagesSent,
                                    LongAccumulator candidateFails,
                                    LongAccumulator tieConflicts,
                                    LongAccumulator iterationsAcc) {
        // Perzistiranje RDD-a za višestruko korištenje
        nodesRDD = nodesRDD.persist(StorageLevel.MEMORY_AND_DISK());

        long nodeCount = nodesRDD.count();
        int maxIterations = Math.min((int) (2 * nodeCount), 500);
        boolean aborted = false;
        int stagnation = 0;

        // Iterativno bojenje dok svi čvorovi ne budu obojeni
        for (int iter = 0; iter < maxIterations; iter++) {
            iterationsAcc.add(1L);

            // Filtriranje neobojenih čvorova (optimizacija: MEMORY_ONLY_SER za privremene RDD-ove)
            JavaPairRDD<Integer, Node> uncoloredRDD = nodesRDD
                    .filter(t -> t._2().color() == -1)
                    .persist(StorageLevel.MEMORY_ONLY_SER());

            long uncoloredCount = uncoloredRDD.count();
            if (uncoloredCount == 0) {
                uncoloredRDD.unpersist();
                nodesRDD.unpersist();
                return true;
            }

            // KORAK 1: prikupljanje boja susjeda (optimizacija: aggregateByKey)
            JavaPairRDD<Integer, Set<Integer>> neighborColors = nodesRDD
                    .filter(t -> t._2().color() != -1)
                    .flatMapToPair(t -> {
                        List<Tuple2<Integer, Integer>> out = new ArrayList<>();
                        int color = t._2().color();
                        for (Integer nb : t._2().neighbors()) {
                            out.add(new Tuple2<>(nb, color));
                        }
                        return out.iterator();
                    })
                    .aggregateByKey(
                            new HashSet<>(),
                            (set, color) -> {
                                set.add(color);
                                return set;
                            },
                            (set1, set2) -> {
                                set1.addAll(set2);
                                return set1;
                            }
                    );

            // KORAK 2: Odabir kandidatske boje (prva slobodna)
            JavaPairRDD<Integer, Integer> candidates = uncoloredRDD.leftOuterJoin(neighborColors)
                    .flatMapToPair(t -> {
                        int nodeId = t._1();
                        Optional<Set<Integer>> opt = t._2()._2();

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

            // KORAK 3: Slanje kandidatskih boja susjedima
            JavaPairRDD<Integer, Tuple2<Integer, Integer>> candidateAnnouncements = candidates.join(nodesRDD)
                    .flatMapToPair(t -> {
                        int nodeId = t._1();
                        int cand = t._2()._1();
                        Node node = t._2()._2();
                        List<Tuple2<Integer, Tuple2<Integer, Integer>>> out = new ArrayList<>();
                        for (Integer nb : node.neighbors()) {
                            out.add(new Tuple2<>(nb, new Tuple2<>(cand, nodeId)));
                        }
                        // Slanje sebi (za provjeru konflikata)
                        out.add(new Tuple2<>(nodeId, new Tuple2<>(cand, nodeId)));
                        messagesSent.add(out.size());
                        return out.iterator();
                    });

            // KORAK 4: Provjera konflikata i odlučivanje
            // groupByKey je neophodan jer se logika odlučivanja oslanja na sve primljene poruke (ne može se reducirati postepeno)
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

                        if (myCand == null)
                            return Collections.emptyListIterator();

                        // Provjera konflikata: ako susjedni čvor ima istu boju i manji ID - odustani
                        for (Tuple2<Integer, Integer> p : list) {
                            if (p._2() != nodeId && p._1().equals(myCand) && p._2() < nodeId) {
                                tieConflicts.add(1);
                                return Collections.emptyListIterator();
                            }
                        }
                        return Collections.singletonList(new Tuple2<>(nodeId, myCand)).iterator();
                    });

            long decidedCount = decided.count();

            // Detekcija stagnacije: ako nema napretka 2 iteracije - neuspjeh
            stagnation = (decidedCount == 0) ? stagnation + 1 : 0;
            if (stagnation >= 2) {
                aborted = true;
                uncoloredRDD.unpersist();
                break;
            }

            // KORAK 5: Ažuriranje stanja čvorova sa novim bojama
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

    /**
     * Pokreće bojenje za fiksni K na zadatom RDD-u.
     * Koristi se za Inkrementalno Bojenje (Zadatak 6).
     */
    public JavaPairRDD<Integer, Node> executeIncremental(JavaPairRDD<Integer, Node> nodesRDD, int k) {
        LOG.info("Pokretanje inkrementalnog bojenja za fiksni k = {}", k);

        // Kreiranje akumulatora za metrike inkrementalnog bojenja
        final LongAccumulator messagesSent = jsc.sc().longAccumulator("incMessagesSent");
        final LongAccumulator candidateFails = jsc.sc().longAccumulator("incCandidateFails");
        final LongAccumulator tieConflicts = jsc.sc().longAccumulator("incTieConflicts");
        final LongAccumulator iterationsAcc = jsc.sc().longAccumulator("incIterations");

        messagesSent.reset();
        candidateFails.reset();
        tieConflicts.reset();
        iterationsAcc.reset();

        // Pokušaj bojenja sa fiksnim K
        boolean success = runColoringForK(nodesRDD, k, messagesSent, candidateFails, tieConflicts, iterationsAcc);

        if (success) {
            LOG.info("Inkrementalno bojenje USPJEŠNO sa K={}. Iteracija: {}", k, iterationsAcc.value());
            return nodesRDD;
        } else {
            LOG.warn("Inkrementalno bojenje NEUSPJEŠNO sa K={}.", k);
            return null;
        }

    }

}