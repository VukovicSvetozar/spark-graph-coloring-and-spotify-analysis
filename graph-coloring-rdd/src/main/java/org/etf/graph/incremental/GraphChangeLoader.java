package org.etf.graph.incremental;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.etf.graph.core.Node;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Klasa za učitavanje i deserijalizaciju promjena grafa iz JSON fajla.
 */
public class GraphChangeLoader {

    private static final Logger LOG = LogManager.getLogger(GraphChangeLoader.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Učitava listu promjena iz JSON fajla.
     */
    public static List<GraphChange> loadChanges(String path) throws IOException {
        LOG.info("Učitavanje promjena iz fajla: {}", path);

        List<GraphChangeDTO> dtos = mapper.readValue(
                new File(path),
                new TypeReference<>() {
                }
        );

        List<GraphChange> changes = new ArrayList<>();
        for (GraphChangeDTO dto : dtos) {
            GraphChange change = convertFromDTO(dto);
            changes.add(change);
        }

        LOG.info("Učitano {} promjena", changes.size());
        return changes;
    }

    /**
     * Konvertuje DTO objekat u GraphChange objekat.
     */
    private static GraphChange convertFromDTO(GraphChangeDTO dto) {
        GraphChangeType type;
        try {
            type = GraphChangeType.valueOf(dto.type.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Nepoznat tip promjene: " + dto.type +
                            ". Dozvoljeni tipovi: ADD_NODE, ADD_EDGE, REMOVE_NODE, REMOVE_EDGE"
            );
        }

        Integer nodeIdA = dto.nodeIdA;
        Integer nodeIdB = dto.nodeIdB;
        Node newNode = null;

        // Validacija i kreiranje Node objekta za ADD_NODE
        if (type == GraphChangeType.ADD_NODE) {
            if (dto.neighbors == null) {
                throw new IllegalArgumentException(
                        "ADD_NODE zahtjeva polje 'neighbors' u JSON-u"
                );
            }
            Set<Integer> neighbors = new HashSet<>(dto.neighbors);
            newNode = new Node(nodeIdA, neighbors, -1);
        }

        // Validacija za operacije sa ivicama
        if ((type == GraphChangeType.ADD_EDGE || type == GraphChangeType.REMOVE_EDGE)
                && nodeIdB == null) {
            throw new IllegalArgumentException(
                    type + " zahtjeva oba polja: nodeIdA i nodeIdB"
            );
        }

        return new GraphChange(type, nodeIdA, nodeIdB, newNode);
    }

    /**
     * DTO klasa za deserijalizaciju JSON-a.
     * Jackson automatski mapira JSON polja na ovu klasu.
     */
    private static class GraphChangeDTO {
        public String type;
        public Integer nodeIdA;
        public Integer nodeIdB;
        public List<Integer> neighbors;
    }

}