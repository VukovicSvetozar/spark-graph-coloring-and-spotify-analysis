package org.etf.graph.incremental;

import java.io.Serial;
import java.io.Serializable;

import org.etf.graph.core.Node;

/**
 * Predstavlja jednu strukturalnu promjenu u grafu (Zadatak 6).
 * type - Tip operacije koja se izvršava
 * nodeIdA - ID prvog čvora (uvijek obavezan)
 * nodeIdB - ID drugog čvora (koristi se samo za ADD_EDGE i REMOVE_EDGE, inače null)
 * newNode - Kompletan Node objekat sa susjedima (koristi se samo za ADD_NODE, inače null)
 */
public record GraphChange(
        GraphChangeType type,
        int nodeIdA,
        Integer nodeIdB,
        Node newNode
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}