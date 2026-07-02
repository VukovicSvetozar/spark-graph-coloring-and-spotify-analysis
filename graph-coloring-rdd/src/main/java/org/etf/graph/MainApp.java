package org.etf.graph;

import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.etf.graph.cli.CLIParser;
import org.etf.graph.cli.HelpRequestedException;
import org.etf.graph.config.GraphConfiguration;
import org.etf.graph.core.Node;
import org.etf.graph.data.ColoringResultSerializer;
import org.etf.graph.data.GraphDataHandler;
import org.etf.graph.data.GraphSerializer;
import org.etf.graph.data.MetricsManager;
import org.etf.graph.incremental.GraphChange;
import org.etf.graph.incremental.GraphChangeLoader;
import org.etf.graph.incremental.GraphChangeType;
import org.etf.graph.incremental.GraphDeltaManager;
import org.etf.graph.jobs.*;
import org.etf.graph.metrics.AverageDegreeResult;
import org.etf.graph.metrics.ColoringResult;
import org.etf.graph.metrics.ValidationResult;

import java.io.IOException;
import java.util.*;

/**
 * Glavna aplikacija (Driver).
 */
public class MainApp {

    private static final Logger LOG = LogManager.getLogger(MainApp.class);

    public static void main(String[] args) {

        // Čišćenje resursa pri gašenju aplikacije (npr. Ctrl+C)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> LOG.info("Pokrenuto kontrolisano gašenje...")));

        GraphConfiguration config;

        // Parsiranje CLI argumenata
        try {
            config = CLIParser.parse(args);
        } catch (HelpRequestedException e) {
            return;
        } catch (ParseException | IllegalArgumentException e) {
            LOG.error("Fatalna greška pri parsiranju CLI argumenata: {}", e.getMessage());
            return;
        }

        // Učitavanje ili generisanje grafa
        List<Node> nodes;
        try {
            nodes = GraphDataHandler.loadOrGenerate(config);
        } catch (Exception e) {
            LOG.error("Fatalna greška pri učitavanju ili generisanju grafa:", e);
            return;
        }

        // Inicijalizacija Spark sesije i konteksta
        SparkSession spark = SparkSession.builder()
                .appName("DistribuiranoBojenjeGrafa")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        ColoringResult coloringResult = null;

        try {

            // ZADATAK 1: Analiza prosječnog stepena grafa
            if (config.runAverageCheck()) {
                AverageDegreeJob avgJob = new AverageDegreeJob(jsc);
                AverageDegreeResult metrics = avgJob.execute(nodes, config);

                LOG.info("=== Rezultati Proračuna Stepena Grafa ===");
                LOG.info("Broj čvorova: {}", nodes.size());
                LOG.info("Maksimalni stepen čvora: {}", metrics.maxDegree());
                LOG.info("Prosječni stepen grafa: {}", metrics.averageDegree());

                if (metrics.executionTimeMs() != null) {
                    LOG.info("Vrijeme izvršavanja: {} ms", metrics.executionTimeMs());
                    if (config.metricsOutPath() != null) {
                        MetricsManager.save(metrics, config.metricsOutPath(), "_avg_degree");
                    }
                }
            }

            // ZADATAK 2 & 5: Distribuirano bojenje grafa (baseline ili optimizovano)
            if (config.runColoring()) {

                // Izbor verzije algoritma
                GraphColoringExecutor colorJobExecutor;
                String versionLabel;

                if (config.useBaseline()) {
                    LOG.info("Koristi se baseline (neoptimizovana) verzija");
                    colorJobExecutor = new GraphColoringJobBaseline(jsc);
                    versionLabel = "BASELINE";
                } else {
                    LOG.info(">>> KORISTI SE optimizovana verzija");
                    colorJobExecutor = new GraphColoringJob(jsc);
                    versionLabel = "OPTIMIZED";
                }

                LOG.info("=== Pokretanje GraphColoringJob [{}] ===", versionLabel);

                // Izvršavanje bojenja
                coloringResult = colorJobExecutor.execute(nodes, config);

                // Logovanje rezultata
                LOG.info("=== Rezultati Bojenja Grafa [{}] ===", versionLabel);
                LOG.info("Vrijeme izvršavanja bojenja (uključujući sve K pokušaje): {} ms", coloringResult.elapsedMs());

                if (coloringResult.success()) {
                    LOG.info("USPJEŠNO: Minimalan broj boja (hromatski broj): {}", coloringResult.bestK());
                } else {
                    LOG.error("NEUSPJEŠNO: Bojenje nije uspjelo ni za jedan K u opsegu (maxDegree+1 do 1).");
                }
                LOG.info("Broj iteracija (ukupno): {}", coloringResult.iterations());
                LOG.info("Broj poruka (ukupno): {}", coloringResult.messagesSent());

                // Izvoz metrika
                if (config.metricsOutPath() != null) {
                    String suffix = config.useBaseline() ? "_coloring_baseline" : "_coloring_optimized";
                    MetricsManager.save(coloringResult, config.metricsOutPath(), suffix);
                }

                // Serijalizacija rezultata (samo ako je bojenje uspješno)
                if (coloringResult.success()) {

                    // Materijalizacija RDD jednom za oba izlaza (optimizacija)
                    List<Node> coloredNodes = null;
                    if (config.outputGraphPath() != null || config.outputResultPath() != null) {
                        LOG.info("Materijalizacija obojenog grafa za serijalizaciju...");
                        coloredNodes = coloringResult.coloredRDD().values().collect();
                        LOG.debug("Materijalizovano {} čvorova.", coloredNodes.size());
                    }

                    // Izlaz 1: Kompletan graf sa susjedima (--output-graph)
                    if (config.outputGraphPath() != null) {
                        LOG.info("Serijalizacija obojenog grafa u fajl: {}", config.outputGraphPath());
                        try {
                            GraphSerializer.saveAsJson(coloredNodes, config.outputGraphPath());
                            LOG.info("Obojeni graf je uspješno serijalizovan.");
                        } catch (IllegalArgumentException e) {
                            LOG.error("Serijalizacija obojenog grafa nije bila uspješna: {}", e.getMessage());
                        }
                    }

                    // Izlaz 2: Kompaktni format (samo nodeId -> color mapping)
                    if (config.outputResultPath() != null) {
                        LOG.info("Snimanje rezultata bojenja u fajl: {}", config.outputResultPath());
                        try {
                            ColoringResultSerializer.saveColoringResult(
                                    coloredNodes,
                                    coloringResult.bestK(),
                                    config.outputResultPath()
                            );
                            LOG.info("Rezultat bojenja je uspješno snimljen.");
                        } catch (IllegalArgumentException e) {
                            LOG.error("Snimanje rezultata bojenja nije bilo uspješno: {}", e.getMessage());
                        }
                    }

                } else {
                    // Bojenje neuspješno - nema se šta snimiti
                    if (config.outputGraphPath() != null || config.outputResultPath() != null) {
                        LOG.warn("Bojenje nije bilo uspješno - izlazni fajlovi neće biti kreirani.");
                    }
                }
            }

            // ZADATAK 3: Validacija bojenja
            if (config.runValidation() && coloringResult != null && coloringResult.coloredRDD() != null) {

                LOG.info("=== Pokretanje GraphValidationJob ===");

                GraphValidationJob validationJob = new GraphValidationJob();
                JavaPairRDD<Integer, Node> coloredRDD = coloringResult.coloredRDD();
                ValidationResult validationMetrics = validationJob.execute(coloredRDD, config);

                LOG.info("=== Rezultati Validacije Bojenja ===");
                LOG.info("Vrijeme izvršavanja validacije: {} ms", validationMetrics.executionTimeMs());
                LOG.info("Broj konflikata (susjedi iste boje): {}", validationMetrics.conflictCount());

                if (validationMetrics.isValid()) {
                    LOG.info("Rezultat validacije: Bojenje je ispravno!");
                } else {
                    LOG.error("Rezultat validacije: Bojenje je neispravno!");
                }

                if (config.metricsOutPath() != null) {
                    MetricsManager.save(validationMetrics, config.metricsOutPath(), "_validation");
                }
            }

            // ZADATAK 6: Inkrementalno bojenje
            if (config.runIncremental() && coloringResult != null && coloringResult.success()) {

                LOG.info("=== Pokretanje GraphDeltaManager ===");

                try {

                    JavaPairRDD<Integer, Node> initialRDD = coloringResult.coloredRDD();
                    int bestK = coloringResult.bestK();

                    // Inicijalizacija delta menadžera za inkrementalne promjene
                    GraphDeltaManager manager = new GraphDeltaManager(initialRDD, jsc, bestK);

                    // Učitavanje promjena: Iz fajla ili fallback na default
                    List<GraphChange> changes;

                    if (config.changesFilePath() != null && !config.changesFilePath().isEmpty()) {
                        // Učitavanje iz JSON fajla
                        LOG.info("Učitavanje promjena iz fajla: {}", config.changesFilePath());
                        changes = GraphChangeLoader.loadChanges(config.changesFilePath());

                    } else {
                        // Fallback: Default demonstracione promjene
                        LOG.warn("Fajl sa promjenama nije naveden (--changes-file). " +
                                "Koriste se default demonstracione promjene.");
                        changes = createDefaultChanges(initialRDD);
                    }

                    // Pokretanje procesiranja svih promjena
                    LOG.info("Pokretanje procesiranje {} promjena...", changes.size());
                    manager.processChanges(changes);

                    // Preuzimanje finalnog stanja grafa
                    JavaPairRDD<Integer, Node> finalRDD = manager.getNodesRDD();

                    LOG.info("=== Rezultati Inkrementalnog Bojenja ===");
                    LOG.info("Broj čvorova poslije promjena: {}", finalRDD.count());

                    coloringResult = new ColoringResult(
                            finalRDD,
                            coloringResult.bestK(),
                            true,
                            coloringResult.elapsedMs(),
                            coloringResult.iterations(),
                            coloringResult.messagesSent(),
                            coloringResult.candidateFails(),
                            coloringResult.tieConflicts()
                    );

                    // Validacija finalnog stanja (opciono)
                    if (config.runValidation()) {
                        LOG.info("Pokretanje validacije finalnog inkrementalno obojenog grafa.");
                        GraphValidationJob finalValidationJob = new GraphValidationJob();
                        ValidationResult finalValidationMetrics = finalValidationJob.execute(finalRDD, config);

                        LOG.info("Finalna validacija: Konflikata={}, Ispravno={}",
                                finalValidationMetrics.conflictCount(),
                                finalValidationMetrics.isValid());

                        if (config.metricsOutPath() != null) {
                            MetricsManager.save(finalValidationMetrics, config.metricsOutPath(), "_incremental_validation");
                        }
                    }

                    // Serijalizacija finalnog stanja poslije inkrementalnog bojenja
                    if (config.outputGraphPath() != null || config.outputResultPath() != null) {
                        LOG.info("Materijalizacija finalnog grafa poslije inkrementalnog bojenja...");
                        List<Node> incrementalNodes = finalRDD.values().collect();

                        if (config.outputGraphPath() != null) {
                            LOG.info("Serijalizacija finalnog grafa u fajl: {}", config.outputGraphPath());
                            try {
                                GraphSerializer.saveAsJson(incrementalNodes, config.outputGraphPath());
                                LOG.info("Finalni graf je uspješno serijalizovan.");
                            } catch (IllegalArgumentException e) {
                                LOG.error("Serijalizacija finalnog grafa nije bila uspješna: {}", e.getMessage());
                            }
                        }

                        if (config.outputResultPath() != null) {
                            LOG.info("Snimanje finalnog rezultata bojenja u fajl: {}", config.outputResultPath());
                            try {
                                ColoringResultSerializer.saveColoringResult(
                                        incrementalNodes,
                                        coloringResult.bestK(),
                                        config.outputResultPath()
                                );
                                LOG.info("Finalni rezultat bojenja je uspješno snimljen.");
                            } catch (IllegalArgumentException e) {
                                LOG.error("Snimanje finalnog rezultata nije bilo uspješno: {}", e.getMessage());
                            }
                        }
                    }

                } catch (IOException e) {
                    LOG.error("Greška pri učitavanju fajla sa promjenama: {}", e.getMessage(), e);
                } catch (Exception e) {
                    LOG.error("Greška tokom izvršavanja inkrementalnog bojenja: {}", e.getMessage(), e);
                }

            } else if (config.runIncremental()) {
                LOG.error("Inkrementalno bojenje nije pokrenuto jer inicijalno bojenje nije uspješno ili nije izvršeno.");
            }

            LOG.info("=== Svi Zadaci Su Uspješno Završeni. ===");

        } catch (Exception e) {
            LOG.error("Fatalna greška tokom izvršavanja Spark poslova: {}", e.getMessage(), e);
        } finally {
            // Čišćenje resursa
            if (coloringResult != null && coloringResult.coloredRDD() != null) {
                LOG.info("Čišćenje RDD rezultata bojenja.");
                coloringResult.coloredRDD().unpersist();
            }

            LOG.info("Zatvaranje Spark sesije.");
            spark.stop();
        }
    }

    /**
     * Kreira default demonstracione promjene
     * Ova metoda se koristi samo ako korisnik ne navede --changes-file.
     */
    private static List<GraphChange> createDefaultChanges(JavaPairRDD<Integer, Node> initialRDD) {
        List<GraphChange> changes = new ArrayList<>();

        // Pronalaženje maksimalnog ID-a u grafu
        int maxNodeId = initialRDD.keys().reduce(Math::max);
        LOG.info("Graf ima maksimalni ID: {}", maxNodeId);

        // PROMJENA A: Brisanje ivice (10, 11) - ako postoji
        if (maxNodeId >= 11) {
            // ✅ ISPRAVNO: Distribuirana provjera da li ivica postoji
            boolean edgeExists = initialRDD
                    .filter(t -> t._1() == 10)
                    .flatMap(t -> {
                        Node node = t._2();
                        if (node.neighbors().contains(11)) {
                            return Collections.singletonList(true).iterator();
                        }
                        return Collections.emptyIterator();
                    })
                    .count() > 0;

            if (edgeExists) {
                changes.add(new GraphChange(GraphChangeType.REMOVE_EDGE, 10, 11, null));
                LOG.info("Dodaje se promjena: Brisanje ivice (10, 11)");
            } else {
                LOG.warn("Ivica (10, 11) ne postoji. Preskače se.");
            }
        }

        // PROMJENA B: Dodavanje novog čvora
        int newNodeId = maxNodeId + 1;
        Set<Integer> newNodeNeighbors = new HashSet<>();
        if (maxNodeId >= 5) newNodeNeighbors.add(5);
        if (maxNodeId >= 10) newNodeNeighbors.add(10);

        Node newNode = new Node(newNodeId, newNodeNeighbors, -1);
        changes.add(new GraphChange(GraphChangeType.ADD_NODE, newNodeId, null, newNode));
        LOG.info("Dodaje se promjena: Dodavanje novog čvora ID={} sa susjedima {}",
                newNodeId, newNodeNeighbors);

        // PROMJENA C: Dodavanje ivice (5, 7) - ako ne postoji
        if (maxNodeId >= 7) {
            boolean canAddEdge = initialRDD
                    .filter(t -> t._1() == 5)
                    .flatMap(t -> {
                        Node node = t._2();
                        // Provjeri da li čvor 5 već ima susjeda 7
                        if (!node.neighbors().contains(7)) {
                            return Collections.singletonList(true).iterator();
                        }
                        return Collections.emptyIterator();
                    })
                    .count() > 0;

            if (canAddEdge) {
                changes.add(new GraphChange(GraphChangeType.ADD_EDGE, 5, 7, null));
                LOG.info("Dodaje se promjena: Dodavanje ivice (5, 7)");
            } else {
                LOG.warn("Ivica (5, 7) već postoji ili čvor 5 ne postoji. Preskače se.");
            }
        }

        // PROMJENA D: Brisanje čvora 3 - samo ako ima mali stepen
        if (maxNodeId >= 3) {
            long node3Degree = initialRDD
                    .filter(t -> t._1() == 3)
                    .map(t -> (long) t._2().degree())
                    .fold(0L, Long::sum);

            if (node3Degree > 0 && node3Degree <= 5) {
                changes.add(new GraphChange(GraphChangeType.REMOVE_NODE, 3, null, null));
                LOG.info("Dodaje se promjena: Brisanje čvora 3 (stepen: {})", node3Degree);
            } else if (node3Degree == 0) {
                LOG.warn("Čvor 3 ne postoji. Preskače se.");
            } else {
                LOG.warn("Čvor 3 ima stepen {}, što je veće od 5. Preskače se.", node3Degree);
            }
        }

        return changes;
    }

}