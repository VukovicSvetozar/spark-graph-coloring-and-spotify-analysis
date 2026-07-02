package org.etf.graph.jobs;

import org.etf.graph.config.GraphConfiguration;
import org.etf.graph.core.Node;
import org.etf.graph.metrics.ColoringResult;

import java.util.List;

/**
 * Zajednički interfejs za sve implementacije distribuiranog bojenja grafa (GraphColoringJob).
 * Osigurava da i optimizovana i baseline verzija imaju isti način pokretanja.
 */
public interface GraphColoringExecutor {

    ColoringResult execute(List<Node> initialNodes, GraphConfiguration config);
}