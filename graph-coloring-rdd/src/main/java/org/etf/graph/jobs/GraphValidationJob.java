package org.etf.graph.jobs;

import org.apache.spark.api.java.JavaPairRDD;
import org.etf.graph.config.GraphConfiguration;
import org.etf.graph.core.GraphValidatorHelper;
import org.etf.graph.core.Node;
import org.etf.graph.core.Timer;
import org.etf.graph.metrics.ValidationResult;

/**
 * Spark job za validaciju ispravnosti bojenja grafa (Zadatak 3).
 * Validacija provjerava da li postoje konfliktne ivice - tj. susjedni čvorovi koji dijele istu boju.
 */
public class GraphValidationJob {

    private static final int DEFAULT_CONFLICT_LOG_SAMPLE = 10;

    /**
     * Izvršava validaciju nad obojenim RDD-om i vraća strukturirani rezultat (broj konflikata i vrijeme izvršavanja).
     */
    public ValidationResult execute(JavaPairRDD<Integer, Node> nodesRDD, GraphConfiguration config) {

        Timer validationTimer = new Timer();
        validationTimer.start();

        // Detekcija konflikata (dobija se ukupan broj konflikata)
        long conflictCount = GraphValidatorHelper.countConflicts(nodesRDD);

        validationTimer.stop();

        boolean isValid = conflictCount == 0;

        if (config.runValidation() && !isValid) {
            // Logovanje ograničenog broja početnh konflikata
            GraphValidatorHelper.validateColoring(nodesRDD, DEFAULT_CONFLICT_LOG_SAMPLE);
        }

        return new ValidationResult(
                isValid,
                conflictCount,
                validationTimer.getElapsedMillis()
        );
    }

}