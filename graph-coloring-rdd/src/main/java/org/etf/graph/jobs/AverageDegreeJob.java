package org.etf.graph.jobs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.etf.graph.config.GraphConfiguration;
import org.etf.graph.core.Node;
import org.etf.graph.core.Timer;
import org.etf.graph.metrics.AverageDegreeResult;

import java.util.List;

/**
 * Spark job za računanje statistike grafa: maksimalni i prosječni stepen čvora.
 */
@SuppressWarnings("ClassCanBeRecord")
public class AverageDegreeJob {

    private static final Logger LOG = LogManager.getLogger(AverageDegreeJob.class);

    private final JavaSparkContext jsc;

    public AverageDegreeJob(JavaSparkContext jsc) {
        this.jsc = jsc;
    }

    public AverageDegreeResult execute(List<Node> nodes, GraphConfiguration config) {

        LOG.debug("Inicijalizacija i pokretanje Spark proračuna za prosječni stepen grafa.");

        // Inicijalizacija štoperice samo ako je --measureTime opcija uključena
        Timer timer = config.measureTime() ? new Timer() : null;

        int partitions = Runtime.getRuntime().availableProcessors();
        JavaRDD<Node> nodesRDD = jsc.parallelize(nodes, partitions);


        if (timer != null)
            timer.start();

        // Optimizacija - cache() za izbjegavanje ponovnog proračuna
        JavaDoubleRDD degreesRDD = nodesRDD.mapToDouble(Node::degree).cache();
        int maxDegree = degreesRDD.max().intValue();
        double averageDegree = degreesRDD.mean();

        if (timer != null)
            timer.stop();

        degreesRDD.unpersist();

        Long timeMs = timer != null ? timer.getElapsedMillis() : null;

        return new AverageDegreeResult(averageDegree, maxDegree, timeMs);

    }

}