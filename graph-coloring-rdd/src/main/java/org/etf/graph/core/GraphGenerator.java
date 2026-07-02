package org.etf.graph.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Klasa za generisanje neusmjerenih, netežinskih grafova.
 * Svaki čvor grafa je predstavljen Node objektom.
 * Graf koji se generiše:
 * - nema petlji
 * - nema duplikata ivica
 * - stepen čvorova ne prelazi zadati maksimalni stepen
 * - lista čvorova je nepromjenjiva
 */
public class GraphGenerator {

    private static final Logger LOG = LogManager.getLogger(GraphGenerator.class);

    /**
     * Generiše neusmjereni graf sa zadatim brojem čvorova i maksimalnim stepenom.
     */
    public static List<Node> generate(int n, int maxDegree, long seed) {
        if (n <= 0)
            throw new IllegalArgumentException("Broj čvorova mora biti veći od nule");
        if (maxDegree < 0)
            throw new IllegalArgumentException("Maksimalni stepen čvora ne može biti negativan");
        if (maxDegree >= n) {
            maxDegree = n - 1;
            LOG.warn("Maksimalni stepen smanjen na {} (N-1) jer je bio veći od broja čvorova.", maxDegree);
        }

        Random rand = new Random(seed);

        // Kreiranje n čvorova (neobojeni i bez susjeda)
        List<Node> nodes = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            nodes.add(new Node(i));
        }

        // Lokalni niz za brzo praćenje stepena svakog čvora (optimizacija)
        int[] degree = new int[n];

        // Generisanje ivica
        for (int i = 0; i < n; i++) {
            Node ni = nodes.get(i);
            for (int j = i + 1; j < n; j++) {
                if (degree[i] >= maxDegree)
                    break;
                if (degree[j] >= maxDegree)
                    continue;

                if (!rand.nextBoolean())
                    continue;

                Node nj = nodes.get(j);
                ni = ni.withAddedNeighbor(j);
                nj = nj.withAddedNeighbor(i);

                nodes.set(i, ni);
                nodes.set(j, nj);

                degree[i]++;
                degree[j]++;
            }
        }

        int actualMaxDegree = 0;
        int totalEdges = 0;
        for (Node node : nodes) {
            actualMaxDegree = Math.max(actualMaxDegree, node.degree());
            totalEdges += node.degree();
        }
        totalEdges /= 2; // svaka ivica je brojana dva puta

        LOG.info("Graf je uspješno generisan: {} čvorova, {} ivica, maksimalni stepen: {} (traženo: {})",
                n, totalEdges, actualMaxDegree, maxDegree);

        return List.copyOf(nodes);
    }

}