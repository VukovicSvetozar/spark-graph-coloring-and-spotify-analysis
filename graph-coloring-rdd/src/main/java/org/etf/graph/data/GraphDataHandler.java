package org.etf.graph.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.etf.graph.config.GraphConfiguration;
import org.etf.graph.core.GraphGenerator;
import org.etf.graph.core.Node;

import java.util.List;

/**
 * Zadužen za učitavanje grafa iz fajla ili generisanje novog grafa.
 * Klasa koristi GraphConfiguration objekat za određivanje i iniciranje potrebne akcije:
 *  - Ako je inputPath naveden -> učitava postojeći graf iz JSON fajla
 *  - Ako su numNodes i maxDegree navedeni -> generiše novi random graf
 *  Opciono serijalizuje graf u JSON format ako je outputGraphPath naveden.
 */
public class GraphDataHandler {

    private static final Logger LOG = LogManager.getLogger(GraphDataHandler.class);

    /**
     * Učitava graf iz JSON fajla ili generiše novi graf na osnovu konfiguracije.
     */
    public static List<Node> loadOrGenerate(GraphConfiguration config) {

        // Učitavanje grafa iz JSON fajla.
         if (config.inputPath() != null && !config.inputPath().isEmpty()) {
            LOG.info("Učitavanje grafa iz JSON fajla: {}", config.inputPath());
            List<Node> nodes = GraphSerializer.loadFromJson(config.inputPath());
            if (nodes.isEmpty())
                throw new IllegalArgumentException("Učitani graf je uspješno pročitan, ali ne sadrži nijedan čvor.");
            LOG.info("Graf je uspješno učitan. Broj čvorova: {}", nodes.size());
            return nodes;
        }

        // Generisanje grafa.
        if (config.numNodes() != null && config.maxDegree() != null) {
            List<Node> nodes = generateGraph(config);
            serializeGraph(nodes, config);  // opciona serijalizacija generisanog grafa
            return nodes;
        }

        throw new IllegalStateException("Konfiguracija grafa je nevažeća. Provjerite da li je u CLI definisan '--input' ili '--generate' sa '--max-degree'.");
    }

    private static List<Node> generateGraph(GraphConfiguration config) throws IllegalArgumentException {
        int n = config.numNodes();
        int maxDegree = config.maxDegree();
        long seed = config.seed();

        LOG.info("Generisanje grafa: n={}, maxDegree={}, seed={}", n, maxDegree, seed);
        List<Node> nodes = GraphGenerator.generate(n, maxDegree, seed);
        LOG.info("Graf je uspješno generisan. Broj čvorova: {}", nodes.size());

        return nodes;
    }

    private static void serializeGraph(List<Node> nodes, GraphConfiguration config) {
        if (config.outputGraphPath() != null) {
            LOG.info("Serijalizacija generisanog grafa u fajl: {}", config.outputGraphPath());
            try {
                GraphSerializer.saveAsJson(nodes, config.outputGraphPath());
                LOG.info("Graf je uspješno serijalizovan.");
            } catch (IllegalArgumentException e) {
                LOG.error("Serijalizacija nije bila uspješna. Generisani graf nije sačuvan: {}", e.getMessage());
            }
        }
    }

}